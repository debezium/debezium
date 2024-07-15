/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenJavaVersion;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class PostgresMetricsIT extends AbstractRecordsProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresMetricsIT.class);

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
        start(YugabyteDBConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
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
        start(YugabyteDBConnector.class,
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
        start(YugabyteDBConnector.class,
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                        .build());

        assertSnapshotMetrics();
        assertStreamingMetrics();
    }

    @Test
    @FixFor("DBZ-6603")
    public void testSnapshotAndStreamingWithCustomMetrics() throws Exception {
        // Setup
        TestHelper.execute(INIT_STATEMENTS, INSERT_STATEMENTS);

        // start connector
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.CUSTOM_METRIC_TAGS, "env=test,bu=bigdata")
                .build();
        Map<String, String> customMetricTags = new PostgresConnectorConfig(config).getCustomMetricTags();
        start(YugabyteDBConnector.class, config);

        assertSnapshotWithCustomMetrics(customMetricTags);
        assertStreamingWithCustomMetrics(customMetricTags);
    }

    @Test
    public void testStreamingOnlyMetrics() throws Exception {
        // Setup
        TestHelper.execute(INIT_STATEMENTS);

        // start connector
        start(YugabyteDBConnector.class,
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
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalTableCount")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "CapturedTables")).isEqualTo(new String[]{ "public.simple" });
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(2L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "RemainingTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotRunning")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotAborted")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPaused")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPausedDurationInSeconds")).isEqualTo(0L);
    }

    private void assertSnapshotWithCustomMetrics(Map<String, String> customMetricTags) throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = getSnapshotMetricsObjectName("postgres", TestHelper.TEST_SERVER, customMetricTags);

        // Wait for the snapshot to complete to verify metrics
        waitForSnapshotWithCustomMetricsToBeCompleted(customMetricTags);

        // Check snapshot metrics
        assertThat(mBeanServer.getAttribute(objectName, "TotalTableCount")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(objectName, "CapturedTables")).isEqualTo(new String[]{ "public.simple" });
        assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(2L);
        assertThat(mBeanServer.getAttribute(objectName, "RemainingTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotRunning")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotAborted")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotCompleted")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotPaused")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotPausedDurationInSeconds")).isEqualTo(0L);
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
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "CapturedTables")).isEqualTo(new String[]{});
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "RemainingTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotRunning")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotAborted")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(false);
    }

    private void assertStreamingMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Wait for the streaming to begin
        TestConsumer consumer = testConsumer(2, "public");
        waitForStreamingToStart();

        // Insert new records and wait for them to become available
        TestHelper.execute(INSERT_STATEMENTS);
        consumer.await(TestHelper.waitTimeForRecords() * 30L, TimeUnit.SECONDS);
        Thread.sleep(Duration.ofSeconds(2).toMillis());

        // Check streaming metrics
        Testing.print("****ASSERTIONS****");
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "Connected")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(2L);
        // todo: this does not seem to be populated?
        // Assertions.assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CapturedTables")).isEqualTo(new String[] {"public.simple"});
    }

    private void assertStreamingWithCustomMetrics(Map<String, String> customMetricTags) throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = getStreamingMetricsObjectName("postgres", TestHelper.TEST_SERVER, customMetricTags);

        // Wait for the streaming to begin
        TestConsumer consumer = testConsumer(2, "public");
        waitForStreamingWithCustomMetricsToStart(customMetricTags);

        // Insert new records and wait for them to become available
        TestHelper.execute(INSERT_STATEMENTS);
        consumer.await(TestHelper.waitTimeForRecords() * 30L, TimeUnit.SECONDS);
        Thread.sleep(Duration.ofSeconds(2).toMillis());

        // Check streaming metrics
        Testing.print("****ASSERTIONS****");
        assertThat(mBeanServer.getAttribute(objectName, "Connected")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(2L);
    }

    @Test
    @SkipWhenJavaVersion(check = EqualityCheck.GREATER_THAN_OR_EQUAL, value = 16, description = "Deep reflection not allowed by default on this Java version")
    public void oneRecordInQueue() throws Exception {
        // Testing.Print.enable();
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        TestHelper.execute(INIT_STATEMENTS, INSERT_STATEMENTS);
        final CountDownLatch step1 = new CountDownLatch(1);
        final CountDownLatch step2 = new CountDownLatch(1);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 10)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, 100L)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES, 10000L);
        start(YugabyteDBConnector.class, configBuilder.build(), loggingCompletion(), null, x -> {
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

        waitForStreamingToStart();
        TestHelper.execute(INSERT_STATEMENTS);
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

    private ObjectName getSnapshotMetricsObjectName() throws MalformedObjectNameException {
        return getSnapshotMetricsObjectName("postgres", TestHelper.TEST_SERVER);
    }

    private ObjectName getStreamingMetricsObjectName() throws MalformedObjectNameException {
        return getStreamingMetricsObjectName("postgres", TestHelper.TEST_SERVER);
    }
}
