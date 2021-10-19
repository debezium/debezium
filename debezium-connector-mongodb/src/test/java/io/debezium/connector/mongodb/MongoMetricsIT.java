/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;

/**
 * @author Chris Cranford
 */
public class MongoMetricsIT extends AbstractMongoConnectorIT {

    @Test
    public void testLifecycle() throws Exception {
        // Setup
        this.config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .build();
        this.context = new MongoDbTaskContext(config);

        // start connector
        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // These methods use the JMX metrics, this simply checks they're available as expected
        waitForSnapshotToBeCompleted("mongodb", "mongo1");
        waitForStreamingRunning("mongodb", "mongo1");

        // Stop the connector
        stopConnector();

        // Verify snapshot metrics no longer exist
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getMBeanInfo(getSnapshotMetricsObjectName("mongodb", "mongo1"));
            fail("Expected Snapshot Metrics no longer to exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }

        // Verify streaming metrics no longer exist
        try {
            mBeanServer.getMBeanInfo(getStreamingMetricsObjectName("mongodb", "mongo1"));
            fail("Expected Streaming Metrics no longer to exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testSnapshotOnlyMetrics() throws Exception {
        // Setup
        this.config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .build();
        this.context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbit");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // start connector
        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // wait for snapshot to have finished
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        SourceRecords records = consumeRecordsByTopic(6);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic("mongo1.dbit.restaurants").size()).isEqualTo(6);

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = getSnapshotMetricsObjectName("mongodb", "mongo1");

        assertThat(mBeanServer.getAttribute(objectName, "TotalTableCount")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(objectName, "RemainingTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotRunning")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotAborted")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotCompleted")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(6L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfEventsFiltered")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfErroneousEvents")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "MonitoredTables")).isEqualTo(new String[]{ "rs0.dbit.restaurants" });
        assertThat(mBeanServer.getAttribute(objectName, "LastEvent")).isNotNull();
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfDisconnects")).isEqualTo(0L);
    }

    @Test
    public void testStreamingOnlyMetrics() throws Exception {
        // Setup
        this.config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NEVER)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .build();
        this.context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbit");

        // start connector
        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // wait for streaming to have started
        waitForStreamingRunning("mongodb", "mongo1");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        SourceRecords records = consumeRecordsByTopic(6);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic("mongo1.dbit.restaurants").size()).isEqualTo(6);

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = getStreamingMetricsObjectName("mongodb", "mongo1");

        assertThat(mBeanServer.getAttribute(objectName, "SourceEventPosition")).isNotNull();
        if (TestHelper.isOplogCaptureMode()) {
            assertThat(mBeanServer.getAttribute(objectName, "NumberOfCommittedTransactions")).isEqualTo(6L);
            assertThat(mBeanServer.getAttribute(objectName, "LastTransactionId")).isNotNull();
        }
        assertThat(mBeanServer.getAttribute(objectName, "Connected")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(objectName, "MonitoredTables")).isEqualTo(new String[]{});
        assertThat(mBeanServer.getAttribute(objectName, "LastEvent")).isNotNull();
        assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(6L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfEventsFiltered")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfErroneousEvents")).isEqualTo(0L);
        assertThat((Long) mBeanServer.getAttribute(objectName, "MilliSecondsSinceLastEvent")).isGreaterThanOrEqualTo(0);
        assertThat((Long) mBeanServer.getAttribute(objectName, "MilliSecondsBehindSource")).isGreaterThanOrEqualTo(0);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfDisconnects")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfPrimaryElections")).isEqualTo(0L);
    }
}
