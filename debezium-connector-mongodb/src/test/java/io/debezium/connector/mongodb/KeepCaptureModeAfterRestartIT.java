/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration test that verifies that if the capture mode is reconfigured it stays the same
 * as the one stored in offsets.
 *
 * @author Jiri Pechanec
 */
public class KeepCaptureModeAfterRestartIT extends AbstractMongoConnectorIT {

    @Rule
    public final TestRule rule = new SkipForOplogTestRule();

    @Test
    public void changeStreamsToOplog() throws Exception {
        // Testing.Print.enable();

        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        testSwitch(CaptureMode.CHANGE_STREAMS_UPDATE_FULL, CaptureMode.OPLOG);

        // stopConnector(value -> assertThat(
        // logInterceptor.containsWarnMessage("Stored offsets were created using change streams capturing.")
        // && logInterceptor.containsWarnMessage("Switching configuration to 'CHANGE_STREAMS_UPDATE_FULL'")
        // && logInterceptor.containsWarnMessage("Either reconfigure the connector or remove the old offsets"))
        // .isTrue());
        stopConnector(value -> assertThat(
                logInterceptor.containsWarnMessage("Stored offsets were created using change streams capturing."))
                        .isTrue());
    }

    @Test
    public void oplogToChangeStreams() throws Exception {
        // Testing.Print.enable();

        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbConnectorTask.class);

        testSwitch(CaptureMode.OPLOG, CaptureMode.CHANGE_STREAMS);

        stopConnector(value -> assertThat(logInterceptor.containsMessage(
                "Stored offsets were created using oplog capturing, trying to switch to change streams.")).isTrue());
    }

    public void testSwitch(CaptureMode from, CaptureMode to) throws InterruptedException {
        config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, from)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        insertDocumentsInTx("dbA", "c1", documentsToInsert.toArray(new Document[0]));

        final SourceRecords records = consumeRecordsByTopic(6);
        final List<SourceRecord> c1s = records.recordsForTopic("mongo1.dbA.c1");
        assertThat(c1s).hasSize(6);

        stopConnector();

        List<Document> documentsToInsert2 = loadTestDocuments("restaurants6.json");
        insertDocuments("dbA", "c1", documentsToInsert2.toArray(new Document[0]));

        config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, to)
                .build();

        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records2 = consumeRecordsByTopic(1);
        final List<SourceRecord> c1s2 = records2.recordsForTopic("mongo1.dbA.c1");
        assertThat(c1s2).hasSize(1);

        final BsonDocument first = BsonDocument.parse(((Struct) c1s2.get(0).value()).getString("after"));
        assertThat(first.getString("restaurant_id").getValue()).isEqualTo("80364347");
    }
}
