/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureScope;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseVersionResolver;
import io.debezium.connector.mongodb.junit.MongoDbPlatform;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;

public class MongoDbConnectorDatabaseRestrictedIT extends AbstractConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectorDatabaseRestrictedIT.class);
    public static final String AUTH_DATABASE = "admin";
    public static final String TEST_DATABASE = "dbit";
    public static final String TEST_COLLECTION = "items";
    public static final String TEST_ALLOWED_USER = "testUser";
    public static final String TEST_ALLOWED_PWD = "testSecret";

    public static final String TOPIC_PREFIX = "mongo";
    private static final int INIT_DOCUMENT_COUNT = 10;
    private static final int NEW_DOCUMENT_COUNT = 4;

    protected static MongoDbReplicaSet mongo;

    @BeforeClass
    public static void beforeAll() {
        Assume.assumeTrue(MongoDbDatabaseVersionResolver.getPlatform().equals(MongoDbPlatform.MONGODB_DOCKER));
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.dockerAuthReplicaSet();
        LOGGER.info("Starting {}...", mongo);
        mongo.start();
        LOGGER.info("Setting up users");
        mongo.createUser(TEST_ALLOWED_USER, TEST_ALLOWED_PWD, AUTH_DATABASE, "read:" + TEST_DATABASE);
    }

    @AfterClass
    public static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            mongo.stop();
        }
    }

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        cleanDatabase(mongo, TEST_DATABASE);
    }

    @After
    public void afterEach() {
        stopConnector();
    }

    protected static MongoClient connect() {
        return MongoClients.create(mongo.getConnectionString());
    }

    protected static void populateCollection(String dbName, String colName, int count) {
        populateCollection(dbName, colName, 0, count);
    }

    protected static void populateCollection(String dbName, String colName, int startId, int count) {
        try (var client = connect()) {
            var db = client.getDatabase(dbName);
            var collection = db.getCollection(colName);

            var items = IntStream.range(startId, startId + count)
                    .mapToObj(i -> new Document("_id", i).append("name", "name_" + i))
                    .collect(Collectors.toList());
            collection.insertMany(items);
        }
    }

    protected Configuration connectorConfiguration(String user, String password) {
        var connectionString = mongo.getAuthConnectionString(user, password, AUTH_DATABASE);
        return TestHelper.getConfiguration(connectionString).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(MongoDbConnectorConfig.CAPTURE_SCOPE, CaptureScope.DATABASE)
                .with(MongoDbConnectorConfig.CAPTURE_TARGET, TEST_DATABASE)
                .build();
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseWithPermissions() throws InterruptedException {
        var documentCount = 0;
        var topic = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION);

        // Populate collection
        populateCollection(TEST_DATABASE, TEST_COLLECTION, INIT_DOCUMENT_COUNT);
        documentCount += INIT_DOCUMENT_COUNT;

        // Use the DB configuration to define the connector's configuration ...
        var config = connectorConfiguration(TEST_ALLOWED_USER, TEST_ALLOWED_PWD);

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        consumeAndVerifyFromInitialSync(topic, INIT_DOCUMENT_COUNT);

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        populateCollection(TEST_DATABASE, TEST_COLLECTION, documentCount, NEW_DOCUMENT_COUNT);
        documentCount += NEW_DOCUMENT_COUNT;

        // Wait until we can consume the documents we just added ...
        consumeAndVerifyNotFromInitialSync(topic, NEW_DOCUMENT_COUNT);
    }

    protected void consumeAndVerifyFromInitialSync(String topic, int expectedRecords) throws InterruptedException {
        var records = consumeRecordsByTopic(expectedRecords);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(topic).size()).isEqualTo(expectedRecords);

        AtomicBoolean foundLast = new AtomicBoolean(false);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSync(record, foundLast);
            verifyOperation(record, Envelope.Operation.READ);
        });
        assertThat(foundLast.get()).isTrue();
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

    protected void consumeAndVerifyNotFromInitialSync(String topic, int expectedRecords) throws InterruptedException {
        consumeAndVerifyNotFromInitialSync(topic, expectedRecords, Envelope.Operation.CREATE);
    }

    protected void consumeAndVerifyNotFromInitialSync(String topic, int expectedRecords, Envelope.Operation op) throws InterruptedException {
        var records = consumeRecordsByTopic(expectedRecords);
        assertThat(records.recordsForTopic(topic).size()).isEqualTo(expectedRecords);
        assertThat(records.topics().size()).isEqualTo(1);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSync(record);
            verifyOperation(record, op);
        });
    }

    protected void verifyNotFromInitialSync(SourceRecord record) {
        assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isFalse();
        Struct value = (Struct) record.value();
        assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isNull();
    }

    protected void verifyOperation(SourceRecord record, Envelope.Operation expected) {
        Struct value = (Struct) record.value();
        assertThat(value.getString(Envelope.FieldName.OPERATION)).isEqualTo(expected.code());
    }
}
