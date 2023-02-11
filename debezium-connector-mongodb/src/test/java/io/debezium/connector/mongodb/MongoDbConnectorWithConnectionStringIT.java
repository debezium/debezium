/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assume;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectorWithConnectionStringIT extends AbstractMongoConnectorIT {

    private Configuration getConfig(String connectionString, boolean ssl) {
        var properties = TestHelper.getConfiguration(mongo).asProperties();
        properties.remove(MongoDbConnectorConfig.HOSTS.name());

        return Configuration.from(properties).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.CONNECTION_STRING, connectionString)
                .with(MongoDbConnectorConfig.SSL_ENABLED, ssl)
                .with(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, true)
                .build();
    }

    @Test
    public void shouldMaskCredentials() {
        config = getConfig("mongodb://admin:password@localhost:27017/", false);
        var connectionContext = new ConnectionContext(config);

        var masked = connectionContext.maskedConnectionSeed();
        assertThat(masked).isEqualTo("mongodb://***:***@localhost:27017/");
    }

    @Test
    public void shouldConsumeAllEventsFromSingleReplicaWithMongoProtocol() throws InterruptedException {
        shouldConsumeAllEventsFromDatabase(mongo.getConnectionString(), false);
    }

    @Test
    public void shouldConsumeAllEventsFromSingleReplicaWithMongoSrvProtocol() throws InterruptedException {
        var connectionString = System.getProperty("mongodb.connection.string");
        Assume.assumeThat(connectionString, notNullValue());
        shouldConsumeAllEventsFromDatabase(connectionString, true);
    }

    public void shouldConsumeAllEventsFromDatabase(String connectionString, boolean ssl) throws InterruptedException {
        config = getConfig(connectionString, ssl);

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, "dbit");

        // Before starting the connector, add data to the databases ...
        storeDocuments("dbit", "simpletons", "simple_objects.json");
        storeDocuments("dbit", "restaurants", "restaurants1.json");

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.simpletons").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(6);
        assertThat(records.topics().size()).isEqualTo(2);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSync(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants2.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records2 = consumeRecordsByTopic(4);
        assertThat(records2.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(4);
        assertThat(records2.topics().size()).isEqualTo(1);
        records2.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSync(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is NOT running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants3.json");

        // ---------------------------------------------------------------------------------------------------------------
        // Start the connector and we should only see the documents added since it was stopped
        // ---------------------------------------------------------------------------------------------------------------
        start(MongoDbConnector.class, config);

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records3 = consumeRecordsByTopic(5);
        assertThat(records3.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(5);
        assertThat(records3.topics().size()).isEqualTo(1);
        records3.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSync(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        storeDocuments("dbit", "restaurants", "restaurants4.json");

        // Wait until we can consume the 4 documents we just added ...
        SourceRecords records4 = consumeRecordsByTopic(8);
        assertThat(records4.recordsForTopic("mongo.dbit.restaurants").size()).isEqualTo(8);
        assertThat(records4.topics().size()).isEqualTo(1);
        records4.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSync(record);
            verifyCreateOperation(record);
        });

        // ---------------------------------------------------------------------------------------------------------------
        // Create and then update a document
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        AtomicReference<String> id = new AtomicReference<>();
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");
            coll.drop();

            // Insert the document with a generated ID ...
            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);

            // Find the document to get the generated ID ...
            doc = coll.find().first();
            Testing.debug("Document: " + doc);
            id.set(doc.getObjectId("_id").toString());
            Testing.debug("Document ID: " + id.get());
        }

        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");
            Document operation = Document.parse("{ \"$set\": { \"b\": 10 } }");
            coll.updateOne(filter, operation);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        }

        // Wait until we can consume the 1 insert and 1 update ...
        SourceRecords insertAndUpdate = consumeRecordsByTopic(2);
        assertThat(insertAndUpdate.recordsForTopic("mongo.dbit.arbitrary").size()).isEqualTo(2);
        assertThat(insertAndUpdate.topics().size()).isEqualTo(1);
        records4.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSync(record);
            verifyCreateOperation(record);
        });
        SourceRecord insertRecord = insertAndUpdate.allRecordsInOrder().get(0);
        SourceRecord updateRecord = insertAndUpdate.allRecordsInOrder().get(1);
        Testing.debug("Insert event: " + insertRecord);
        Testing.debug("Update event: " + updateRecord);
        Struct insertKey = (Struct) insertRecord.key();
        Struct updateKey = (Struct) updateRecord.key();
        String insertId = toObjectId(insertKey.getString("id")).toString();
        String updateId = toObjectId(updateKey.getString("id")).toString();
        assertThat(insertId).isEqualTo(id.get());
        assertThat(updateId).isEqualTo(id.get());

        // ---------------------------------------------------------------------------------------------------------------
        // Delete a document
        // ---------------------------------------------------------------------------------------------------------------
        try (var client = connect()) {
            MongoDatabase db1 = client.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");
            Document filter = Document.parse("{\"a\": 1}");
            coll.deleteOne(filter);
        }

        // Wait until we can consume the 1 delete ...
        SourceRecords delete = consumeRecordsByTopic(2);
        assertThat(delete.recordsForTopic("mongo.dbit.arbitrary").size()).isEqualTo(2);
        assertThat(delete.topics().size()).isEqualTo(1);

        SourceRecord deleteRecord = delete.allRecordsInOrder().get(0);
        validate(deleteRecord);
        verifyNotFromInitialSync(deleteRecord);
        verifyDeleteOperation(deleteRecord);

        SourceRecord tombStoneRecord = delete.allRecordsInOrder().get(1);
        validate(tombStoneRecord);

        Testing.debug("Delete event: " + deleteRecord);
        Testing.debug("Tombstone event: " + tombStoneRecord);
        Struct deleteKey = (Struct) deleteRecord.key();
        String deleteId = toObjectId(deleteKey.getString("id")).toString();
        assertThat(deleteId).isEqualTo(id.get());
    }

    protected void verifyFromInitialSync(SourceRecord record, AtomicBoolean foundLast) {
        if (record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)) {
            assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isTrue();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        }
        else {
            // Only the last record in the initial sync should be marked as not being part of the initial sync ...
            assertThat(foundLast.getAndSet(true)).isFalse();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
        }
    }

    protected void verifyNotFromInitialSync(SourceRecord record) {
        assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isFalse();
        Struct value = (Struct) record.value();
        assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    protected void verifyCreateOperation(SourceRecord record) {
        verifyOperation(record, Operation.CREATE);
    }

    protected void verifyReadOperation(SourceRecord record) {
        verifyOperation(record, Operation.READ);
    }

    protected void verifyDeleteOperation(SourceRecord record) {
        verifyOperation(record, Operation.DELETE);
    }

    protected void verifyOperation(SourceRecord record, Operation expected) {
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(expected.code());
    }

    protected void storeDocuments(String dbName, String collectionName, String pathOnClasspath) {
        try (var client = connect()) {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = client.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            storeDocuments(coll, pathOnClasspath);
        }
    }

    protected void storeDocuments(MongoCollection<Document> collection, String pathOnClasspath) {
        InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
        loadTestDocuments(pathOnClasspath).forEach(doc -> {
            assertThat(doc).isNotNull();
            assertThat(doc.size()).isGreaterThan(0);
            collection.insertOne(doc, insertOptions);
        });
    }

    protected List<Document> loadTestDocuments(String pathOnClasspath) {
        List<Document> results = new ArrayList<>();
        try (InputStream stream = Files.readResourceAsStream(pathOnClasspath)) {
            assertThat(stream).isNotNull();
            IoUtil.readLines(stream, line -> {
                Document doc = Document.parse(line);
                assertThat(doc.size()).isGreaterThan(0);
                results.add(doc);
            });
        }
        catch (IOException e) {
            fail("Unable to find or read file '" + pathOnClasspath + "': " + e.getMessage());
        }
        return results;
    }

    private String formatObjectId(ObjectId objId) {
        return "{\"$oid\": \"" + objId + "\"}";
    }

    private ObjectId toObjectId(String oid) {
        return new ObjectId(oid.substring(10, oid.length() - 2));
    }
}
