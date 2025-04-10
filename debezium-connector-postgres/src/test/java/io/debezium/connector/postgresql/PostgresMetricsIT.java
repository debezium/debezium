/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenJavaVersion;
import io.debezium.pipeline.AbstractMetricsTest;

/**
 * @author Chris Cranford
 * @author Mario Fiore Vitale
 */
public class PostgresMetricsIT extends AbstractMetricsTest<PostgresConnector> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresMetricsIT.class);

    private static final String INIT_STATEMENTS = "CREATE TABLE simple (pk SERIAL NOT NULL, val INT NOT NULL, PRIMARY KEY(pk)); "
            + "ALTER TABLE simple REPLICA IDENTITY FULL;";
    private static final String INSERT_STATEMENTS = "INSERT INTO simple (val) VALUES (25); "
            + "INSERT INTO simple (val) VALUES (50);";

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return "test_server";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);
    }

    protected Configuration.Builder noSnapshot(Configuration.Builder config) {
        return config.with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA);
    }

    @Override
    protected void executeInsertStatements() {
        TestHelper.execute(INSERT_STATEMENTS);
    }

    @Override
    protected String tableName() {
        return "public.simple";
    }

    @Override
    protected long expectedEvents() {
        return 2L;
    }

    @Override
    protected long snapshotCompleted() {
        return 0L;
    }

    @Before
    public void before() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();

        TestHelper.execute(INIT_STATEMENTS);
    }

    @After
    public void after() throws Exception {
        stopConnector();
    }

    @Test
    @SkipWhenJavaVersion(check = EqualityCheck.GREATER_THAN_OR_EQUAL, value = 16, description = "Deep reflection not allowed by default on this Java version")
    public void oneRecordInQueue() throws Exception {
        // Testing.Print.enable();
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        executeInsertStatements();
        final CountDownLatch step1 = new CountDownLatch(1);
        final CountDownLatch step2 = new CountDownLatch(1);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 10)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, 100L)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES, 10000L);
        start(PostgresConnector.class, configBuilder.build(), loggingCompletion(), null, x -> {
            LOGGER.info("Record '{}' arrived", x);
            step1.countDown();
            try {
                step2.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Record processing completed");
        }, true);

        waitForStreamingRunning(connector(), server());
        executeInsertStatements();
        LOGGER.info("Waiting for the first record to arrive");
        step1.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS);
        LOGGER.info("First record arrived");

        // One record is read from queue and reading is blocked
        // Second record should stay in queue
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    long value = (long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
                    return value > 0;
                });
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isNotEqualTo(0L);

        LOGGER.info("Wait for the queue to contain second record");
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    int value = (int) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity");
                    return value == 9;
                });
        LOGGER.info("Wait for second record to be in queue");
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity")).isEqualTo(9);

        LOGGER.info("Empty queue");
        step2.countDown();

        LOGGER.info("Wait for queue to be empty");
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    long value = (long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
                    return value == 0;
                });
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isEqualTo(0L);
        stopConnector();
    }

}
