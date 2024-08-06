/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.TabularDataSupport;

import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

public abstract class AbstractMetricsTest<T extends SourceConnector> extends AbstractConnectorTest {

    protected abstract Class<T> getConnectorClass();

    protected abstract String connector();

    protected abstract String server();

    protected abstract Configuration.Builder config();

    protected abstract Configuration.Builder noSnapshot(Configuration.Builder config);

    protected abstract void executeInsertStatements() throws Exception;

    protected abstract String tableName();

    protected abstract long expectedEvents();

    protected abstract boolean snapshotCompleted();

    protected String task() {
        return null;
    }

    protected String database() {
        return null;
    }

    protected void start() {
        final Configuration config = config().build();
        start(getConnectorClass(), config, loggingCompletion(), null);
    }

    protected void start(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        final Configuration config = custConfig.apply(config()).build();
        start(getConnectorClass(), config, loggingCompletion(), null);
    }

    @Test
    public void testLifecycle() throws Exception {

        start();

        assertConnectorIsRunning();

        // These methods use the JMX metrics, this simply checks they're available as expected
        waitForSnapshotToBeCompleted(connector(), server(), task(), database());
        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

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
        executeInsertStatements();

        // start connector
        start();

        assertSnapshotMetrics();
    }

    @Test
    public void testSnapshotAndStreamingMetrics() throws Exception {
        // Setup
        executeInsertStatements();

        // start connector
        start();
        assertConnectorIsRunning();

        assertSnapshotMetrics();
        assertStreamingMetrics(false, expectedEvents());
    }

    @Test
    @FixFor("DBZ-6603")
    public void testSnapshotAndStreamingWithCustomMetrics() throws Exception {
        // Setup
        executeInsertStatements();

        // start connector

        Map<String, String> customMetricTags = Map.of("env", "test", "bu", "bigdata");
        start(x -> x.with(CommonConnectorConfig.CUSTOM_METRIC_TAGS, "env=test,bu=bigdata"));

        assertSnapshotWithCustomMetrics(customMetricTags);
        assertStreamingWithCustomMetrics(customMetricTags, expectedEvents());
    }

    @Test
    public void testStreamingOnlyMetrics() throws Exception {

        // start connector
        start(this::noSnapshot);

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());
        assertSnapshotNotExecutedMetrics();
        assertStreamingMetrics(false, expectedEvents());
    }

    @Test
    public void testAdvancedStreamingMetrics() throws Exception {

        // start connector
        start(x -> noSnapshot(x)
                .with(CommonConnectorConfig.ADVANCED_METRICS_ENABLE, Boolean.TRUE));

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());
        assertSnapshotNotExecutedMetrics();
        assertStreamingMetrics(true, expectedEvents());
    }

    @Test
    public void testPauseAndResumeAdvancedStreamingMetrics() throws Exception {

        // start connector
        start(x -> noSnapshot(x)
                .with(CommonConnectorConfig.ADVANCED_METRICS_ENABLE, Boolean.TRUE));

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());
        assertSnapshotNotExecutedMetrics();
        assertStreamingMetrics(true, expectedEvents());

        invokeOperation(getMultiplePartitionStreamingMetricsObjectName(), "pause");
        insertRecords();
        assertAdvancedMetrics(2);

        invokeOperation(getMultiplePartitionStreamingMetricsObjectName(), "resume");
        insertRecords();
        assertAdvancedMetrics(4);
    }

    private void insertRecords() throws Exception {
        // Wait for the streaming to begin
        executeInsertStatements();
        waitForAvailableRecords(30, TimeUnit.SECONDS);
        Thread.sleep(Duration.ofSeconds(2).toMillis());
    }

    protected void assertSnapshotMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Wait for the snapshot to complete to verify metrics
        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        // Check snapshot metrics
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalTableCount")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "CapturedTables")).isEqualTo(new String[]{ tableName() });
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(2L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "RemainingTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotRunning")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotAborted")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPaused")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPausedDurationInSeconds")).isEqualTo(0L);
    }

    protected void assertSnapshotWithCustomMetrics(Map<String, String> customMetricTags) throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = getSnapshotMetricsObjectName(connector(), server(), task(), database(), customMetricTags);

        // Wait for the snapshot to complete to verify metrics
        waitForSnapshotWithCustomMetricsToBeCompleted(connector(), server(), task(), database(), customMetricTags);

        // Check snapshot metrics
        assertThat(mBeanServer.getAttribute(objectName, "TotalTableCount")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(objectName, "CapturedTables")).isEqualTo(new String[]{ tableName() });
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

        Awaitility.await("Waiting for snapshot metrics to appear").atMost(waitTimeForRecords(), TimeUnit.SECONDS).until(() -> {
            try {
                mBeanServer.getObjectInstance(getSnapshotMetricsObjectName());
                return true;
            }
            catch (InstanceNotFoundException e) {
                return false;
            }
        });

        // Check snapshot metrics
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(snapshotCompleted());
    }

    protected void assertStreamingMetrics(boolean checkAdvancedMetrics, long expectedEvents) throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        waitForStreamingRunning(connector(), server(), getStreamingNamespace(), task());

        // Insert new records and wait for them to become available
        executeInsertStatements();
        // Testing.Print.enable();
        consumeRecordsByTopic((int) expectedEvents);
        Thread.sleep(Duration.ofSeconds(2).toMillis());

        // Check streaming metrics
        Testing.print("****ASSERTIONS****");
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "Connected")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(getMultiplePartitionStreamingMetricsObjectName(), "TotalNumberOfCreateEventsSeen")).isEqualTo(expectedEvents);

        if (checkAdvancedMetrics) {
            assertAdvancedMetrics(2L);
        }
    }

    public void assertAdvancedMetrics(long expectedInsert) throws Exception {

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        TabularDataSupport numberOfCreateEventsSeen = (TabularDataSupport) mBeanServer
                .getAttribute(getStreamingMetricsObjectName(connector(), server(), getStreamingNamespace(), task(), database()), "NumberOfCreateEventsSeen");

        String values = numberOfCreateEventsSeen.values().stream()
                .limit(1)
                .toList()
                .get(0)
                .toString();
        assertThat(values).isEqualTo(
                "javax.management.openmbean.CompositeDataSupport(compositeType=javax.management.openmbean.CompositeType(name=java.util.Map<java.lang.String, java.lang.Long>,items=((itemName=key,itemType=javax.management.openmbean.SimpleType(name=java.lang.String)),(itemName=value,itemType=javax.management.openmbean.SimpleType(name=java.lang.Long)))),contents={key="
                        + tableName() + ", value="
                        + expectedInsert + "})");
    }

    private void invokeOperation(ObjectName objectName, String operation)
            throws ReflectionException, InstanceNotFoundException, MBeanException {

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        server.invoke(objectName, operation, new Object[]{}, new String[]{});
    }

    protected void assertStreamingWithCustomMetrics(Map<String, String> customMetricTags, long expectedEvents) throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        waitForStreamingWithCustomMetricsToStart(connector(), server(), task(), database(), customMetricTags);

        // Insert new records and wait for them to become available
        executeInsertStatements();
        waitForAvailableRecords(30, TimeUnit.SECONDS);
        consumeRecords((int) expectedEvents);
        Thread.sleep(Duration.ofSeconds(2).toMillis());

        // Check streaming metrics
        Testing.print("****ASSERTIONS****");
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(connector(), server(), task(), null, customMetricTags), "Connected")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(getMultiplePartitionStreamingMetricsObjectNameCustomTags(customMetricTags), "TotalNumberOfCreateEventsSeen"))
                .isEqualTo(expectedEvents);
    }

    protected ObjectName getSnapshotMetricsObjectName() throws MalformedObjectNameException {
        return getSnapshotMetricsObjectName(connector(), server());
    }

    protected ObjectName getStreamingMetricsObjectName() throws MalformedObjectNameException {
        return getStreamingMetricsObjectName(connector(), server());
    }

    protected ObjectName getMultiplePartitionStreamingMetricsObjectName() throws MalformedObjectNameException {
        // Only SQL Server manage partition scoped metrics
        return getStreamingMetricsObjectName(connector(), server());
    }

    protected ObjectName getMultiplePartitionStreamingMetricsObjectNameCustomTags(Map<String, String> customTags) throws MalformedObjectNameException {
        // Only SQL Server manage partition scoped metrics
        return getStreamingMetricsObjectName(connector(), server(), customTags);
    }
}
