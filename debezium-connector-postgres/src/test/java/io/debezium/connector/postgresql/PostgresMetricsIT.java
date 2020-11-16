/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;

/**
 * @author Chris Cranford
 */
public class PostgresMetricsIT extends AbstractRecordsProducerTest {

    private static final String INIT_STATEMENTS = "CREATE TABLE simple (pk SERIAL NOT NULL, val INT NOT NULL, PRIMARY KEY(pk)); "
            + "ALTER TABLE simple REPLICA IDENTITY FULL;";
    private static final String INSERT_STATEMENTS = "INSERT INTO simple (val) VALUES (25); "
            + "INSERT INTO simple (val) VALUES (50);";

    @Before
    public void before() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();
    }

    @After
    public void after() throws Exception {
        stopConnector();
    }

    @Test
    public void testLifecycle() throws Exception {
        // start connector
        start(PostgresConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                        .build());

        assertConnectorIsRunning();

        // These methods use the JMX metrics, this simply checks they're available as expected
        waitForSnapshotToBeCompleted();
        waitForStreamingToStart();

        // Stop the connector
        stopConnector();

        // Verify snapshot metrics no longer exist
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getMBeanInfo(getSnapshotMetricsObjectName());
            Assert.fail("Expected Snapshot Metrics no longer to exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }

        // Verify streaming metrics no longer exist
        try {
            mBeanServer.getMBeanInfo(getStreamingMetricsObjectName());
            Assert.fail("Expected Streaming Metrics no longer to exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testSnapshotOnlyMetrics() throws Exception {
        // Setup
        TestHelper.execute(INIT_STATEMENTS, INSERT_STATEMENTS);

        // start connector
        start(PostgresConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                        .build());

        assertSnapshotMetrics();
    }

    @Test
    public void testSnapshotAndStreamingMetrics() throws Exception {
        // Setup
        TestHelper.execute(INIT_STATEMENTS, INSERT_STATEMENTS);

        // start connector
        start(PostgresConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                        .build());

        assertSnapshotMetrics();
        assertStreamingMetrics();
    }

    @Test
    public void testStreamingOnlyMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Setup
        TestHelper.execute(INIT_STATEMENTS);

        // start connector
        start(PostgresConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                        .build());

        assertSnapshotNotExecutedMetrics();
        assertStreamingMetrics();
    }

    private void assertSnapshotMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Wait for the snapshot to complete to verify metrics
        waitForSnapshotToBeCompleted();

        // Check snapshot metrics
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalTableCount")).isEqualTo(1);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "MonitoredTables")).isEqualTo(new String[]{ "public.simple" });
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(2L);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "RemainingTableCount")).isEqualTo(0);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotRunning")).isEqualTo(false);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotAborted")).isEqualTo(false);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(true);
    }

    private void assertSnapshotNotExecutedMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        Awaitility.await("Waiting for snapshot metrics to appear").atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS).until(() -> {
            try {
                mBeanServer.getObjectInstance(getSnapshotMetricsObjectName());
                return true;
            }
            catch (InstanceNotFoundException e) {
                return false;
            }
        });

        // Check snapshot metrics
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalTableCount")).isEqualTo(0);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "MonitoredTables")).isEqualTo(new String[]{});
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(0L);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "RemainingTableCount")).isEqualTo(0);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotRunning")).isEqualTo(false);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotAborted")).isEqualTo(false);
        Assertions.assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(false);
    }

    private void assertStreamingMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Wait for the streaming to begin
        TestConsumer consumer = testConsumer(2, "public");
        waitForStreamingToStart();

        // Insert new records and wait for them to become available
        TestHelper.execute(INSERT_STATEMENTS);
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
        Thread.sleep(Duration.ofSeconds(2).toMillis());

        // Check streaming metrics
        System.out.println("****ASSERTIONS****");
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "Connected")).isEqualTo(true);
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(2L);
        // todo: this does not seem to be populated?
        // Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "MonitoredTables")).isEqualTo(new String[] {"public.simple"});
    }

    @Test
    public void twoRecordsInQueue() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        TestHelper.execute(INIT_STATEMENTS, INSERT_STATEMENTS);
        final int recordCount = 2;

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 10)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, 5000L)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES, 10000L);
        start(PostgresConnector.class, configBuilder.build());

        waitForStreamingToStart();
        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STATEMENTS);
        }
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    long value = (long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
                    return value > 0;
                });
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isNotEqualTo(0L);
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    int value = (int) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity");
                    return value == 8;
                });
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity")).isEqualTo(8);

        SourceRecords records = consumeRecordsByTopic(recordCount);

        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isEqualTo(0L);
        stopConnector();
    }

    @Test
    public void oneRecordInQueue() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        TestHelper.execute(INIT_STATEMENTS, INSERT_STATEMENTS);
        final int recordCount = 2;

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 10)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, 5000L)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES, 10L);
        start(PostgresConnector.class, configBuilder.build());

        waitForStreamingToStart();
        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STATEMENTS);
        }
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    long value = (long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
                    return value > 0;
                });
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isNotEqualTo(0L);
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    int value = (int) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity");
                    return value == 9;
                });
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity")).isEqualTo(9);

        SourceRecords records = consumeRecordsByTopic(recordCount);
        Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isEqualTo(0L);
        stopConnector();
    }

    private ObjectName getSnapshotMetricsObjectName() throws MalformedObjectNameException {
        return getSnapshotMetricsObjectName("postgres", TestHelper.TEST_SERVER);
    }

    private ObjectName getStreamingMetricsObjectName() throws MalformedObjectNameException {
        return getStreamingMetricsObjectName("postgres", TestHelper.TEST_SERVER);
    }
}
