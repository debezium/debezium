/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import io.debezium.config.Configuration;

/**
 * Test to verify multi-thread incremental snapshotting for MongoDB.
 *
 * @author Yue Wang
 */
public class MultiThreadIncrementalSnapshotIT extends IncrementalSnapshotIT {

    protected static final int ROW_COUNT = 1_000;
    private static final int INCREMENTAL_SNAPSHOT_THREADS = 7; // use a prime number in tests to cover the cases where the last chunk is less than the chunk size.

    @Override
    protected Configuration.Builder config() {
        Configuration.Builder builder = super.config();
        return builder.with(MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS, INCREMENTAL_SNAPSHOT_THREADS);
    }

    @Test
    public void multiThreadingSnapshot() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startConnector();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    public void multiThreadSnapshotWithRestart() throws Exception {
        // Testing.Print.enable();

        populateDataCollection();
        startAndConsumeTillEnd(connectorClass(), config().build());
        waitForConnectorToStart();

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final AtomicInteger recordCounter = new AtomicInteger();
        final AtomicBoolean restarted = new AtomicBoolean();
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount, x -> true,
                x -> {
                    if (recordCounter.addAndGet(x.size()) > 50 && !restarted.get()) {
                        stopConnector();
                        assertConnectorNotRunning();

                        // restart connector with different threads count to make sure there is still no data loss.
                        start(connectorClass(), super.config().with(MongoDbConnectorConfig.SNAPSHOT_MAX_THREADS, INCREMENTAL_SNAPSHOT_THREADS + 2).build());
                        waitForConnectorToStart();
                        restarted.set(true);
                    }
                });
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }
}
