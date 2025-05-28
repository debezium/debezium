/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

public abstract class InnoDBMvccSnapshot<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path DB_HISTORY_PATH = Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String INSERT_STATEMENT = "INSERT INTO test_table (name,email,last_updated) values ('John Doe%d','johndoe@gmail.com',NOW())";
    protected final UniqueDatabase DATABASE = TestHelper
            .getUniqueDatabase("simulation", "innodb_mvcc")
            .withDbHistoryPath(DB_HISTORY_PATH);

    @Before
    public void beforeEach() {
        stopConnector();

        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(DB_HISTORY_PATH);
        }
    }

    protected abstract String snapshotMode();

    @Test
    @FixFor("DBZ-9006")
    public void shouldUseReadLockMvccWhenDoingSnapshot() throws Exception {
        start(getConnectorClass(), DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.MAX_QUEUE_SIZE, 150_000)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 0)
                .with("snapshot.locking.mode", snapshotMode())
                .build());

        assertConnectorIsRunning();

        ExecutorService executorService = executeScheduledStatement(1000,
                100L,
                100,
                DATABASE.getDatabaseName(),
                INSERT_STATEMENT);

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        AtomicReference<SourceRecord> duplicate = consumeEvents();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(duplicate.get()).isNull());

        executorService.shutdownNow();
        stopConnector();
    }

    protected AtomicReference<SourceRecord> consumeEvents() {
        final Map<Integer, SourceRecord> records = new LinkedHashMap<>();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

        AtomicReference<SourceRecord> duplicate = new AtomicReference<>(null);

        scheduledExecutorService.scheduleAtFixedRate(
                () -> consumeAvailableRecords((record) -> {
                    final Struct value = (Struct) record.value();
                    if (value != null && value.schema().field("after") != null) {
                        final int id = value.getStruct(Envelope.FieldName.AFTER).getInt32("id");
                        SourceRecord prev = records.put(id, record);
                        if (prev != null) {
                            duplicate.set(prev);
                        }
                    }
                }), 0, 5, TimeUnit.NANOSECONDS);

        return duplicate;
    }

}
