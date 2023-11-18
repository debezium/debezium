/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.HeaderFrom;
import org.apache.kafka.connect.transforms.InsertHeader;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * Test to verify MongoDB connector behaviour with CloudEvents converter for all streaming events.
 *
 * @author Jiri Pechanec
 */
public class CloudEventsConverterIT extends AbstractMongoConnectorIT {

    protected static final String SERVER_NAME = "mongo1";
    protected static final String DB_NAME = "dbA";
    protected static final String COLLECTION_NAME = "c1";

    @Before
    public void beforeEach() {
        Testing.Print.enable();
        config = getConfiguration();
        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(mongo, DB_NAME);
        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();
    }

    @Test
    public void testCorrectFormat() throws Exception {
        // Wait for snapshot completion
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        insertDocuments(DB_NAME, COLLECTION_NAME, documentsToInsert.toArray(new Document[0]));
        Document updateObj = new Document()
                .append("$set", new Document()
                        .append("name", "Closed"));
        updateDocument(DB_NAME, COLLECTION_NAME, Document.parse("{\"restaurant_id\": \"30075445\"}"), updateObj);
        // Pause is necessary to make sure that fullDocument fro change streams is caputred before delete
        Thread.sleep(1000);
        deleteDocuments(DB_NAME, COLLECTION_NAME, Document.parse("{\"restaurant_id\": \"30075445\"}"));

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
        CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(deleteRecord, "mongodb", "mongo1", false);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJson(updateRecord, false);
        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(updateRecord, MongoDbFieldName.UPDATE_DESCRIPTION, false);
        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(updateRecord, Envelope.FieldName.AFTER, false);
        CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(updateRecord, "mongodb", "mongo1", false);
    }

    @Test
    @FixFor({ "DBZ-6982" })
    public void shouldConvertToCloudEventsInJsonWithoutExtensionAttributes() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                    .insertOne(new Document()
                            .append("pk", 1)
                            .append("aa", 1));
        }

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic("mongo1.dbA.c1").get(0);

        assertThat(record).isNotNull();
        assertThat(record.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithoutExtensionAttributes(record);
    }

    @Test
    @FixFor({ "DBZ-3642" })
    public void shouldConvertToCloudEventsInJsonWithMetadataAndIdAndTypeInHeadersAfterOutboxEventRouter() throws Exception {
        HeaderFrom<SourceRecord> headerFrom = new HeaderFrom.Value<>();
        Map<String, String> headerFromConfig = new LinkedHashMap<>();
        headerFromConfig.put("fields", "source,op,transaction");
        headerFromConfig.put("headers", "source,op,transaction");
        headerFromConfig.put("operation", "copy");
        headerFromConfig.put("header.converter.schemas.enable", "true");
        headerFrom.configure(headerFromConfig);

        MongoEventRouter<SourceRecord> outboxEventRouter = new MongoEventRouter<>();
        Map<String, String> outboxEventRouterConfig = new LinkedHashMap<>();
        outboxEventRouterConfig.put("collection.expand.json.payload", "true");
        // this adds `id` and `type` headers with value from the DB column
        outboxEventRouterConfig.put("collection.fields.additional.placement", "event_type:header:type,id:header:id");
        outboxEventRouter.configure(outboxEventRouterConfig);

        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                    .insertOne(new Document()
                            .append("id", "59a42efd-b015-44a9-9dde-cb36d9002425")
                            .append("aggregateid", "10711fa5")
                            .append("aggregatetype", "User")
                            .append("event_type", "UserCreated")
                            .append("payload", new Document()
                                    .append("_id", new ObjectId("000000000000000000000000"))
                                    .append("someField1", "some value 1")
                                    .append("someField2", 7005L)));
        }

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic("mongo1.dbA.c1").get(0);
        SourceRecord recordWithMetadataHeaders = headerFrom.apply(record);
        SourceRecord routedEvent = outboxEventRouter.apply(recordWithMetadataHeaders);

        assertThat(routedEvent).isNotNull();
        assertThat(routedEvent.topic()).isEqualTo("outbox.event.User");
        assertThat(routedEvent.keySchema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(routedEvent.key()).isEqualTo("10711fa5");
        assertThat(routedEvent.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithMetadataAndIdAndTypeInHeaders(routedEvent, "mongodb", "mongo1");

        headerFrom.close();
        outboxEventRouter.close();
    }

    @Test
    @FixFor({ "DBZ-7016" })
    public void shouldConvertToCloudEventsInJsonWithGeneratedIdAndTypeFromHeader() throws Exception {
        InsertHeader<SourceRecord> insertHeader = new InsertHeader<>();
        Map<String, String> insertHeaderConfig = new LinkedHashMap<>();
        insertHeaderConfig.put("header", "id");
        insertHeaderConfig.put("value.literal", "77742efd-b015-44a9-9dde-cb36d9002425");
        insertHeader.configure(insertHeaderConfig);

        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                    .insertOne(new Document()
                            .append("pk", 1)
                            .append("aa", 1));
        }

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic("mongo1.dbA.c1").get(0);
        SourceRecord recordWithTypeInHeader = insertHeader.apply(record);

        assertThat(recordWithTypeInHeader).isNotNull();
        assertThat(recordWithTypeInHeader.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithIdFromHeaderAndGeneratedType(recordWithTypeInHeader, "mongodb", "mongo1");

        insertHeader.close();
    }

    @Test
    @FixFor({ "DBZ-7159" })
    public void shouldThrowExceptionWhenDeserializingNotCloudEventJson() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                    .insertOne(new Document()
                            .append("pk", 1)
                            .append("aa", 1));
        }

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic("mongo1.dbA.c1").get(0);

        assertThat(record).isNotNull();
        assertThat(record.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldThrowExceptionWhenDeserializingNotCloudEventJson(record);
    }

    @Test
    @FixFor({ "DBZ-7159" })
    public void shouldThrowExceptionWhenDeserializingNotCloudEventAvro() throws Exception {
        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                    .insertOne(new Document()
                            .append("pk", 1)
                            .append("aa", 1));
        }

        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        assertThat(streamingRecords.allRecordsInOrder()).hasSize(1);

        SourceRecord record = streamingRecords.recordsForTopic("mongo1.dbA.c1").get(0);

        assertThat(record).isNotNull();
        assertThat(record.value()).isInstanceOf(Struct.class);

        CloudEventsConverterTest.shouldThrowExceptionWhenDeserializingNotCloudEventAvro(record);
    }

    private Configuration getConfiguration() {
        return TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DB_NAME + "." + COLLECTION_NAME)
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .build();
    }
}
