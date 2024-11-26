/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.AbstractMongoConnectorIT;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.data.Envelope;

/**
 * Baseline for all integrations tests regarding MongoDB Update Operations
 *
 * @author Renato Mefi
 */
public abstract class AbstractExtractNewDocumentStateTestIT extends AbstractMongoConnectorIT {

    protected static final String DB_NAME = "transform_operations";
    protected static final String SERVER_NAME = "mongo";

    // for ExtractNewDocumentStateTestIT
    protected static final String CONFIG_DROP_TOMBSTONES = "drop.tombstones";
    protected static final String HANDLE_DELETES = "delete.handling.mode";
    protected static final String HANDLE_TOMBSTONE_DELETES = "delete.tombstone.handling.mode";
    protected static final String FLATTEN_STRUCT = "flatten.struct";
    protected static final String DELIMITER = "flatten.struct.delimiter";
    protected static final String DROP_TOMBSTONE = "drop.tombstones";
    protected static final String ADD_HEADERS = "add.headers";
    protected static final String ADD_FIELDS = "add.fields";
    protected static final String ADD_FIELDS_PREFIX = ADD_FIELDS + ".prefix";
    protected static final String ADD_HEADERS_PREFIX = ADD_HEADERS + ".prefix";
    protected static final String ARRAY_ENCODING = "array.encoding";

    protected ExtractNewDocumentState<SourceRecord> transformation;

    protected abstract String getCollectionName();

    protected String topicName() {
        return String.format("%s.%s.%s", SERVER_NAME, DB_NAME, this.getCollectionName());
    }

    @Before
    public void beforeEach() {
        // Use the DB configuration to define the connector's configuration ...
        Configuration config = getBaseConfigBuilder().build();

        beforeEach(config);
    }

    public void beforeEach(Configuration config) {
        super.beforeEach();

        transformation = new ExtractNewDocumentState<>();
        transformation.configure(Collections.emptyMap());

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, DB_NAME);

        // Start the connector ...
        start(MongoDbConnector.class, config);
    }

    @After
    public void afterEach() {
        super.afterEach();
        transformation.close();
    }

    protected void restartConnectorWithoutEmittingTombstones() throws InterruptedException {
        // make sure connector is fully running
        waitForStreamingRunning();
        // stop connector
        afterEach();

        // reconfigure and restart
        Configuration config = getBaseConfigBuilder()
                .with(MongoDbConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        beforeEach(config);
    }

    protected Configuration.Builder getBaseConfigBuilder() {
        return TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DB_NAME + "." + this.getCollectionName())
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME);
    }

    protected void restartConnectorWithConfig(Configuration config) throws InterruptedException {
        // make sure connector is fully running
        waitForStreamingRunning();

        // stop connector
        afterEach();

        beforeEach(config);
    }

    SourceRecord getRecordByOperation(Envelope.Operation operation) throws InterruptedException {
        final SourceRecord candidateRecord = getNextRecord();

        if (!((Struct) candidateRecord.value()).get("op").equals(operation.code())) {
            // MongoDB is not providing really consistent snapshot, so the initial insert
            // can arrive both in initial sync snapshot and in oplog
            return getRecordByOperation(operation);
        }

        return candidateRecord;
    }

    SourceRecord getNextRecord() throws InterruptedException {
        SourceRecords records = consumeRecordsByTopic(1);

        assertThat(records.recordsForTopic(this.topicName()).size()).isEqualTo(1);

        return records.recordsForTopic(this.topicName()).get(0);
    }

    protected SourceRecord getUpdateRecord() throws InterruptedException {
        return getRecordByOperation(Envelope.Operation.UPDATE);
    }

    // for ExtractNewDocumentStateTestIT
    protected SourceRecords createCreateRecordFromJson(String pathOnClasspath) throws Exception {
        final List<Document> documents = loadTestDocuments(pathOnClasspath);
        try (var client = connect()) {
            for (Document document : documents) {
                client.getDatabase(DB_NAME).getCollection(getCollectionName()).insertOne(document);
            }
        }

        final SourceRecords records = consumeRecordsByTopic(documents.size());
        assertThat(records.recordsForTopic(topicName()).size()).isEqualTo(documents.size());
        assertNoRecordsToConsume();

        return records;
    }

    protected SourceRecord createCreateRecord() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("address", new Document()
                        .append("struct", "Morris Park Ave")
                        .append("zipcode", "10462"));

        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(getCollectionName()).insertOne(obj);
        }

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(topicName()).size()).isEqualTo(1);
        assertNoRecordsToConsume();

        return records.allRecordsInOrder().get(0);
    }

    protected SourceRecords createDeleteRecordWithTombstone() throws Exception {
        ObjectId objId = new ObjectId();
        Document obj = new Document()
                .append("_id", objId)
                .append("name", "Sally")
                .append("address", new Document()
                        .append("struct", "Morris Park Ave")
                        .append("zipcode", "10462"));

        try (var client = connect()) {
            client.getDatabase(DB_NAME).getCollection(getCollectionName()).insertOne(obj);
        }

        final SourceRecords createRecords = consumeRecordsByTopic(1);
        assertThat(createRecords.recordsForTopic(topicName()).size()).isEqualTo(1);
        assertNoRecordsToConsume();

        try (var client = connect()) {
            Document filter = Document.parse("{\"_id\": {\"$oid\": \"" + objId + "\"}}");
            client.getDatabase(DB_NAME).getCollection(getCollectionName()).deleteOne(filter);
        }

        final SourceRecords deleteRecords = consumeRecordsByTopic(2);
        assertThat(deleteRecords.recordsForTopic(topicName()).size()).isEqualTo(2);
        assertNoRecordsToConsume();

        return deleteRecords;
    }

    protected static void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning("mongodb", SERVER_NAME);
    }

    protected String getSourceRecordHeaderByKey(SourceRecord record, String headerKey) {
        Iterator<Header> headers = record.headers().allWithName(headerKey);
        if (!headers.hasNext()) {
            return null;
        }
        Object value = headers.next().value();
        return value != null ? value.toString() : null;
    }
}
