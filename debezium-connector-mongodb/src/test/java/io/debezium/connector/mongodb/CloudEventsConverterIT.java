/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Test;

import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.util.Testing;

/**
 * Test to verify MongoDB connector behaviour with CloudEvents converter for all streaming events.
 *
 * @author Jiri Pechanec
 */
public class CloudEventsConverterIT extends AbstractMongoConnectorIT {

    @Test
    public void testCorrectFormat() throws Exception {
        Testing.Print.enable();
        config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        insertDocuments("dbA", "c1", documentsToInsert.toArray(new Document[0]));
        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Closed"));
        updateDocument("dbA", "c1", Document.parse("{\"restaurant_id\": \"30075445\"}"), updateObj);
        // Pause is necessary to make sure that fullDocument fro change streams is caputred before delete
        Thread.sleep(1000);
        deleteDocuments("dbA", "c1", Document.parse("{\"restaurant_id\": \"30075445\"}"));

        // 6 INSERTs + 1 UPDATE + 1 DELETE
        final int recCount = 8;
        final SourceRecords records = consumeRecordsByTopic(recCount);
        final List<SourceRecord> c1s = records.recordsForTopic("mongo1.dbA.c1");

        assertThat(c1s).hasSize(recCount);

        final List<SourceRecord> insertRecords = c1s.subList(0, 6);
        final SourceRecord updateRecord = c1s.get(6);
        final SourceRecord deleteRecord = c1s.get(7);

        for (SourceRecord record : insertRecords) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "mongodb", "mongo1", false);
        }

        CloudEventsConverterTest.shouldConvertToCloudEventsInJson(deleteRecord, false);
        if (TestHelper.isOplogCaptureMode()) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(deleteRecord, "filter", false);
        }
        CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(deleteRecord, "mongodb", "mongo1", false);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJson(updateRecord, false);
        if (TestHelper.isOplogCaptureMode()) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(updateRecord, "filter", false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(updateRecord, "patch", false);
        }
        else {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(updateRecord, "after", false);
        }
        CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(updateRecord, "mongodb", "mongo1", false);

        stopConnector();
    }
}
