/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.util.JSON;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectorIT extends AbstractConnectorTest {

    private Configuration config;
    private MongoDbTaskContext context;

    @Before
    public void beforeEach() {
        Testing.Debug.disable();
        Testing.Print.disable();
        stopConnector();
        initializeConnectorTestFramework();
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

    /**
     * Verifies that the connector doesn't run with an invalid configuration. This does not actually connect to the MySQL server.
     */
    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        config = Configuration.create()
                .with(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, "true")
                .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldFailToValidateInvalidConfiguration() {
        Configuration config = Configuration.create().build();
        MongoDbConnector connector = new MongoDbConnector();
        Config result = connector.validate(config.asMap());

        assertConfigurationErrors(result, MongoDbConnectorConfig.HOSTS, 1);
        assertConfigurationErrors(result, MongoDbConnectorConfig.LOGICAL_NAME, 1);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.USER);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.PASSWORD);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_WHITELIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_BLACKLIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_WHITELIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_BLACKLIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_COPY_THREADS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.CONNECT_BACKOFF_INITIAL_DELAY_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.CONNECT_BACKOFF_MAX_DELAY_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ENABLED);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);
        assertNoConfigurationErrors(result, CommonConnectorConfig.TOMBSTONES_ON_DELETE);
    }

    @Test
    public void shouldValidateAcceptableConfiguration() {
        config = TestHelper.getConfiguration();

        // Add data to the databases so that the databases will be listed ...
        context = new MongoDbTaskContext(config);
        storeDocuments("dbval", "validationColl1", "simple_objects.json");
        storeDocuments("dbval2", "validationColl2", "restaurants1.json");

        MongoDbConnector connector = new MongoDbConnector();
        Config result = connector.validate(config.asMap());

        assertNoConfigurationErrors(result, MongoDbConnectorConfig.HOSTS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.LOGICAL_NAME);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.USER);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.PASSWORD);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_WHITELIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.DATABASE_BLACKLIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_WHITELIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.COLLECTION_BLACKLIST);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_COPY_THREADS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_QUEUE_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_BATCH_SIZE);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.POLL_INTERVAL_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.CONNECT_BACKOFF_INITIAL_DELAY_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.CONNECT_BACKOFF_MAX_DELAY_MS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ENABLED);
        assertNoConfigurationErrors(result, MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);
        assertNoConfigurationErrors(result, CommonConnectorConfig.TOMBSTONES_ON_DELETE);
    }

    @Test
    public void shouldConsumeAllEventsFromDatabase() throws InterruptedException, IOException {

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, "dbit.*")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(primary(), "dbit");

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
        primary().execute("create", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
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
        });

        primary().execute("update", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("arbitrary");

            // Find the document ...
            Document doc = coll.find().first();
            Testing.debug("Document: " + doc);
            Document filter = Document.parse("{\"a\": 1}");
            Document operation = Document.parse("{ \"$set\": { \"b\": 10 } }");
            coll.updateOne(filter, operation);

            doc = coll.find().first();
            Testing.debug("Document: " + doc);
        });

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
        String insertId = JSON.parse(insertKey.getString("id")).toString();
        String updateId = JSON.parse(updateKey.getString("id")).toString();
        assertThat(insertId).isEqualTo(id.get());
        assertThat(updateId).isEqualTo(id.get());
    }

    @Test
    @FixFor("DBZ-865 and DBZ-1242")
    public void shouldConsumeEventsFromCollectionWithReplacedTopicName() throws InterruptedException, IOException {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, "dbit.dbz865.*")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(primary(), "dbit");

        primary().execute("create", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("dbz865_my@collection");
            coll.drop();

            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        });

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        SourceRecords records = consumeRecordsByTopic(12);
        records.topics().forEach(System.out::println);
        assertThat(records.recordsForTopic("mongo.dbit.dbz865_my_collection")).hasSize(1);
        assertThat(records.topics().size()).isEqualTo(1);
        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSync(record, foundLast);
            verifyReadOperation(record);
        });
        assertThat(foundLast.get()).isTrue();

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isFalse());
    }

    @Test
    @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingCollectionFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        // Use the DB configuration to define the connector's configuration...
        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, "dbit.dbz865.my_products")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .build();

        // Set up the replication context for connections...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(primary(), "dbit");

        primary().execute("create", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("dbz865_my@collection");
            coll.drop();

            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        });

        // Start the connector...
        start(MongoDbConnector.class, config);

        // Consume all records
        consumeRecordsByTopic(12);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isTrue());
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

    protected void verifyUpdateOperation(SourceRecord record) {
        verifyOperation(record, Operation.UPDATE);
    }

    protected void verifyDeleteOperation(SourceRecord record) {
        verifyOperation(record, Operation.DELETE);
    }

    protected void verifyOperation(SourceRecord record, Operation expected) {
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(expected.code());
    }

    protected MongoPrimary primary() {
        ReplicaSet replicaSet = ReplicaSet.parse(context.getConnectionContext().hosts());
        return context.getConnectionContext().primaryFor(replicaSet, context.filters(), connectionErrorHandler(3));
    }

    protected void storeDocuments(String dbName, String collectionName, String pathOnClasspath) {
        primary().execute("storing documents", mongo -> {
            Testing.debug("Storing in '" + dbName + "." + collectionName + "' documents loaded from from '" + pathOnClasspath + "'");
            MongoDatabase db1 = mongo.getDatabase(dbName);
            MongoCollection<Document> coll = db1.getCollection(collectionName);
            coll.drop();
            storeDocuments(coll, pathOnClasspath);
        });
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
        try (InputStream stream = Testing.Files.readResourceAsStream(pathOnClasspath);) {
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

    protected BiConsumer<String, Throwable> connectionErrorHandler(int numErrorsBeforeFailing) {
        AtomicInteger attempts = new AtomicInteger();
        return (desc, error) -> {
            if (attempts.incrementAndGet() > numErrorsBeforeFailing) {
                fail("Unable to connect to primary after " + numErrorsBeforeFailing + " errors trying to " + desc + ": " + error);
            }
            logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
        };
    }

    @Test(expected = ConnectException.class)
    public void shouldUseSSL() throws InterruptedException, IOException {
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, "dbit.*")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .with(MongoDbConnectorConfig.MAX_FAILED_CONNECTIONS, 0)
                .with(MongoDbConnectorConfig.SSL_ENABLED, true)
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        primary().executeBlocking("Try SSL connection", mongo -> {
            mongo.getDatabase("dbit");
        });
    }

    @Test
    @FixFor("DBZ-1198")
    public void shouldEmitHeartbeatMessages() throws InterruptedException, IOException {
        Testing.Print.enable();
        // Use the DB configuration to define the connector's configuration ...
        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, "dbit.mhb")
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(primary(), "dbit");

        primary().execute("create", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
            MongoCollection<Document> coll1 = db1.getCollection("mhb");
            coll1.drop();
            Document doc = Document.parse("{\"a\": 1, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll1.insertOne(doc, insertOptions);

            MongoCollection<Document> coll2 = db1.getCollection("nmhb");
            coll2.drop();
        });

        // Start the connector ...
        start(MongoDbConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);
        assertThat(records.recordsForTopic("mongo.dbit.mhb")).hasSize(1);

        primary().execute("insert-monitored", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("mhb");

            Document doc = Document.parse("{\"a\": 2, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        });

        // Monitored collection event followed by heartbeat
        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic("mongo.dbit.mhb")).hasSize(1);
        final Map<String, ?> monitoredOffset = records.recordsForTopic("mongo.dbit.mhb").get(0).sourceOffset();
        final Integer monitoredTs = (Integer) monitoredOffset.get(SourceInfo.TIMESTAMP);
        final Integer monitoredOrd = (Integer) monitoredOffset.get(SourceInfo.ORDER);
        assertThat(records.recordsForTopic("__debezium-heartbeat.mongo")).hasSize(1);
        final Map<String, ?> hbAfterMonitoredOffset = records.recordsForTopic("__debezium-heartbeat.mongo").get(0).sourceOffset();
        assertThat(monitoredTs).isEqualTo((Integer) hbAfterMonitoredOffset.get(SourceInfo.TIMESTAMP));
        assertThat(monitoredOrd).isEqualTo((Integer) hbAfterMonitoredOffset.get(SourceInfo.ORDER));

        primary().execute("insert-nonmonitored", mongo -> {
            MongoDatabase db1 = mongo.getDatabase("dbit");
            MongoCollection<Document> coll = db1.getCollection("nmhb");

            Document doc = Document.parse("{\"a\": 3, \"b\": 2}");
            InsertOneOptions insertOptions = new InsertOneOptions().bypassDocumentValidation(true);
            coll.insertOne(doc, insertOptions);
        });

        // Heartbeat created by non-monitored collection event and heartbeat created by MongoDB heartbeat event
        records = consumeRecordsByTopic(2);
        final List<SourceRecord> heartbeatRecords = records.recordsForTopic("__debezium-heartbeat.mongo");
        assertThat(heartbeatRecords.size()).isGreaterThanOrEqualTo(1);
        heartbeatRecords.forEach(record -> {
            // Offset of the heartbeats should be greater than of the last monitored event
            final Map<String, ?> offset = record.sourceOffset();
            final Integer ts = (Integer) offset.get(SourceInfo.TIMESTAMP);
            final Integer ord = (Integer) offset.get(SourceInfo.ORDER);
            assertThat(ts > monitoredTs || (ts == monitoredTs && ord > monitoredOrd));
        });
        stopConnector();
    }
}
