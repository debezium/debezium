/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Chris Cranford
 */
public class MongoMetricsIT extends AbstractMongoConnectorIT {

    @Test
    public void testLifecycle() throws Exception {
        // Setup
        this.config = TestHelper.getConfiguration(mongo)
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
        this.config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .build();
        this.context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");
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
        assertThat(mBeanServer.getAttribute(objectName, "CapturedTables")).isEqualTo(new String[]{ "dbit.restaurants" });
        assertThat(mBeanServer.getAttribute(objectName, "LastEvent")).isNotNull();
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfDisconnects")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotPaused")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(objectName, "SnapshotPausedDurationInSeconds")).isEqualTo(0L);
    }

    @Test
    public void testStreamingOnlyMetrics() throws Exception {
        // Setup
        this.config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NEVER)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .build();
        this.context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");

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
        assertThat(mBeanServer.getAttribute(objectName, "Connected")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(objectName, "CapturedTables")).isEqualTo(new String[]{});
        assertThat(mBeanServer.getAttribute(objectName, "LastEvent")).isNotNull();
        assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(6L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfEventsFiltered")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfErroneousEvents")).isEqualTo(0L);
        assertThat((Long) mBeanServer.getAttribute(objectName, "MilliSecondsSinceLastEvent")).isGreaterThanOrEqualTo(0);
        assertThat((Long) mBeanServer.getAttribute(objectName, "MilliSecondsBehindSource")).isGreaterThanOrEqualTo(0);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfDisconnects")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(objectName, "NumberOfPrimaryElections")).isEqualTo(0L);
    }

    @Test
    public void testPauseResumeSnapshotMetrics() throws Exception {
        final String DOCUMENT_ID = "_id";
        final int NUM_RECORDS = 1_000;

        this.config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NEVER)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, "dbit.debezium_signal")
                .with(MongoDbConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1)
                .build();
        this.context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbit");
        final Document[] documents = new Document[NUM_RECORDS];
        for (int i = 0; i < NUM_RECORDS; i++) {
            final Document doc = new Document();
            doc.append(DOCUMENT_ID, i + 1).append("aa", i);
            documents[i] = doc;
        }
        insertDocumentsInTx("dbit", "numbers", documents);

        // Start connector.
        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("mongodb", "mongo1");

        // Start incremental snapshot.
        insertDocuments("dbit", "debezium_signal", new Document[]{ Document.parse(
                "{\"type\": \"execute-snapshot\", \"payload\": \"{\\\"data-collections\\\": [\\\" dbit.numbers \\\"]}\"}") });

        // Pause incremental snapshot.
        insertDocuments("dbit", "debezium_signal", new Document[]{ Document.parse(
                "{\"type\": \"pause-snapshot\", \"payload\": \"{}\"}") });

        // Sleep more than 1 second, we get the pause in seconds.
        Thread.sleep(1500);

        // Resume incremental snapshot.
        insertDocuments("dbit", "debezium_signal", new Document[]{ Document.parse(
                "{\"type\": \"resume-snapshot\", \"payload\": \"{}\"}") });

        // Consume incremental snapshot records.
        List<SourceRecord> records = new ArrayList<>();
        consumeRecords(NUM_RECORDS, record -> {
            records.add(record);
        });
        Assert.assertTrue(records.size() >= NUM_RECORDS);

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final ObjectName objectName = getSnapshotMetricsObjectName("mongodb", "mongo1");
        final long snapshotPauseDuration = (Long) mBeanServer.getAttribute(objectName, "SnapshotPausedDurationInSeconds");
        Assert.assertTrue(snapshotPauseDuration > 0);
    }
}
