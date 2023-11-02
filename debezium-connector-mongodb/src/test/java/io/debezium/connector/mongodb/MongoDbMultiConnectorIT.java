/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.JsonSerialization.COMPACT_JSON_SETTINGS;
import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;

public class MongoDbMultiConnectorIT extends AbstractMongoConnectorIT {

    /**
     * Verifies that the connector doesn't run with an invalid configuration. This does not actually connect to the Mongo server.
     */

    static String db = "multiDb";
    static String collection = "multiCol";

    @Before
    public void beforeEach() {
        initializeConnectorTestFramework();

        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, db + ".*")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), db);

    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            if (context != null) {
                context.getConnectionContext().shutdown();
            }
        }
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        config = Configuration.create()
                .with(MongoDbConnectorConfig.STREAMING_SHARDS, -1)
                .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    protected void startConnector(Configuration c) {
        start(MongoDbConnector.class, c);
        //waitForConnectorToStart();
        waitForStreamingRunning("mongodb", "mongo");

        waitForAvailableRecords(5, TimeUnit.SECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    }

    @Test
    public void multiConnectorshouldGenerateRecordForInsertEventOplog() throws Exception {
        int streamingShardCount = 4;
        int process = 1; // Currently IT only support single connectors
        ArrayList<Configuration> configs = new ArrayList<>(streamingShardCount);
        for (int i = 0; i < streamingShardCount; i++) {
            Configuration c =Configuration.copy(config)
                    .with(MongoDbConnectorConfig.STREAMING_SHARDS, streamingShardCount)
                    .with(MongoDbConnectorConfig.STREAMING_SHARD_ID, i)
                    .with(MongoDbConnectorConfig.CAPTURE_MODE, "oplog")
                    .build();

            configs.add(c);
        }

        start(MongoDbConnector.class, configs.get(0));
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        int nRecords = 100;
        populateDataCollection(nRecords);

        // Consume all record inserts
        int totalRecordsRead = 0;
        for (int c = 0; c < process; c++) {
            final SourceRecords record = consumeRecordsByTopic(nRecords, true);

            totalRecordsRead += record.allRecordsInOrder().size();
            stopConnector();
        }

        assertThat(totalRecordsRead).isEqualTo(nRecords/streamingShardCount);
        assertNoRecordsToConsume();
    }

    @Test
    public void multiConnectorshouldGenerateRecordForInsertEvent() throws Exception {
        int streamingShardCount = 4;
        int process = 1; // Currently IT only support single connectors
        ArrayList<Configuration> configs = new ArrayList<>(streamingShardCount);
        for (int i = 0; i < streamingShardCount; i++) {
            Configuration c =Configuration.copy(config)
                    .with(MongoDbConnectorConfig.STREAMING_SHARDS, streamingShardCount)
                    .with(MongoDbConnectorConfig.STREAMING_SHARD_ID, i)
                    .build();

            configs.add(c);
        }

        start(MongoDbConnector.class, configs.get(0));
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        int nRecords = 100;
        populateDataCollection(nRecords);

        // Consume all record inserts
        int totalRecordsRead = 0;
        for (int c = 0; c < process; c++) {
            final SourceRecords record = consumeRecordsByTopic(nRecords, true);

            totalRecordsRead += record.allRecordsInOrder().size();
            stopConnector();
        }

        assertThat(totalRecordsRead).isEqualTo(nRecords/streamingShardCount);
        assertNoRecordsToConsume();
    }

    @Test
    public void singleConnectorshouldGenerateRecordForInsertEvent() throws Exception {
        config = Configuration.copy(config)
                .with(MongoDbConnectorConfig.STREAMING_SHARDS, 1)
                .with(MongoDbConnectorConfig.STREAMING_SHARD_ID, 0)
                .build();

        context = new MongoDbTaskContext(config);

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert record
        final Instant timestamp = Instant.now();
        Document obj = populateDataCollection(1).get(0);

        // Consume records, should be 1, the insert
        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(1);
        assertNoRecordsToConsume();

        final SourceRecord deleteRecord = records.allRecordsInOrder().get(0);
        final Struct key = (Struct) deleteRecord.key();
        final Struct value = (Struct) deleteRecord.value();

        assertThat(key.schema()).isSameAs(deleteRecord.keySchema());
        assertThat(key.get("id")).isEqualTo(formatObjectId(obj.getObjectId("_id")));

        assertThat(value.schema()).isSameAs(deleteRecord.valueSchema());
        assertThat(value.getString(Envelope.FieldName.AFTER)).isEqualTo(obj.toJson(COMPACT_JSON_SETTINGS));
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.getInt64(Envelope.FieldName.TIMESTAMP)).isGreaterThanOrEqualTo(timestamp.toEpochMilli());
    }

    private ArrayList<Document> populateDataCollection(int nRecords) {
        ArrayList<Document> docs = new ArrayList<>();
        for (int i = 0; i < nRecords; i++) {
            final Instant timestamp = Instant.now();
            ObjectId objId = new ObjectId();
            Document obj = new Document("_id", objId).append("appTime", timestamp.toEpochMilli());
            insertDocuments(db, collection, obj);
            docs.add(obj);
        }

        return docs;
    }

    private String formatObjectId(ObjectId objId) {
        return "{\"$oid\": \"" + objId + "\"}";
    }
}
