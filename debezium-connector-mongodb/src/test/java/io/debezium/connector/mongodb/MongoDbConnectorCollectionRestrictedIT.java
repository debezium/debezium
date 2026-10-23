/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.AssertionsForClassTypes;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseVersionResolver;
import io.debezium.connector.mongodb.junit.MongoDbPlatform;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;

public class MongoDbConnectorCollectionRestrictedIT extends AbstractAsyncEngineConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectorCollectionRestrictedIT.class);
    public static final String AUTH_DATABASE = "admin";
    public static final String TEST_DATABASE = "dbit";
    public static final String TEST_USER = "testUser";
    public static final String TEST_PWD = "testSecret";
    public static final String TEST_COLLECTION1 = "items1";
    public static final String TEST_COLLECTION2 = "items2";

    // scoped to items1 only
    public static final String COLLECTION_ONE_ROLE = "collectionOneScopedRole";
    public static final String COLLECTION_ONE_USER = "collectionOneScopedUser";
    public static final String COLLECTION_ONE_PWD = "collectionOneScopedSecret";

    // scoped to items2 only
    public static final String COLLECTION_TWO_ROLE = "collectionTwoScopedRole";
    public static final String COLLECTION_TWO_USER = "collectionTwoScopedUser";
    public static final String COLLECTION_TWO_PWD = "collectionTwoScopedSecret";

    // scoped to a collection in a database that is never populated, so it never physically exists
    public static final String OTHER_DATABASE = "dbother";
    public static final String OTHER_DATABASE_COLLECTION = "itemsOther";
    public static final String OTHER_DATABASE_ROLE = "otherDatabaseScopedRole";
    public static final String OTHER_DATABASE_USER = "otherDatabaseScopedUser";
    public static final String OTHER_DATABASE_PWD = "otherDatabaseScopedSecret";

    public static final String TOPIC_PREFIX = "mongo";
    private static final int INIT_DOCUMENT_COUNT = 10;
    protected static MongoDbReplicaSet mongo;

    @BeforeAll
    static void beforeAll() {
        Assumptions.assumeTrue(MongoDbDatabaseVersionResolver.getPlatform().equals(MongoDbPlatform.MONGODB_DOCKER));
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.dockerAuthReplicaSet();
        LOGGER.info("Starting {}...", mongo);
        mongo.start();
        LOGGER.info("Setting up users");
        mongo.createUser(TEST_USER, TEST_PWD, AUTH_DATABASE, "read:" + TEST_DATABASE);

        var scopedActions = List.of("find", "changeStream", "listIndexes", "collStats");

        mongo.createRole(COLLECTION_ONE_ROLE, TEST_DATABASE, TEST_COLLECTION1, scopedActions);
        mongo.createUser(COLLECTION_ONE_USER, COLLECTION_ONE_PWD, AUTH_DATABASE, COLLECTION_ONE_ROLE + ":" + TEST_DATABASE);

        mongo.createRole(COLLECTION_TWO_ROLE, TEST_DATABASE, TEST_COLLECTION2, scopedActions);
        mongo.createUser(COLLECTION_TWO_USER, COLLECTION_TWO_PWD, AUTH_DATABASE, COLLECTION_TWO_ROLE + ":" + TEST_DATABASE);

        mongo.createRole(OTHER_DATABASE_ROLE, OTHER_DATABASE, OTHER_DATABASE_COLLECTION, scopedActions);
        mongo.createUser(OTHER_DATABASE_USER, OTHER_DATABASE_PWD, AUTH_DATABASE, OTHER_DATABASE_ROLE + ":" + OTHER_DATABASE);
    }

    @AfterAll
    static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            mongo.stop();
        }
    }

    @BeforeEach
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        cleanDatabase(mongo, TEST_DATABASE);
    }

    @AfterEach
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

    protected Configuration connectorConfiguration(String user, String password, String captureTarget) {
        var connectionString = mongo.getAuthConnectionString(user, password, AUTH_DATABASE);
        return TestHelper.getConfiguration(connectionString).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 2)
                .with(MongoDbConnectorConfig.CAPTURE_SCOPE, MongoDbConnectorConfig.CaptureScope.COLLECTION)
                .with(MongoDbConnectorConfig.CAPTURE_TARGET, captureTarget)
                .build();
    }

    @Test
    @FixFor("dbz#2716")
    public void shouldConsumeEventsFromSingleCollectionWithScopedAccess() throws InterruptedException {
        var topic = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION1);

        // populate collection
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration(COLLECTION_ONE_USER, COLLECTION_ONE_PWD, TEST_DATABASE + "." + TEST_COLLECTION1);

        // start the connector using an account with privileges on items1 only
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(INIT_DOCUMENT_COUNT);
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic).size()).isEqualTo(INIT_DOCUMENT_COUNT);
    }

    @Test
    @FixFor("dbz#2716")
    public void shouldNotConsumeEventsFromCollectionWithoutScopeUsingScopedAccess() throws InterruptedException {
        LogInterceptor logInterceptor = new LogInterceptor(MongoUtils.class);
        var topic1 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION1);
        var topic2 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION2);

        // populate collection
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);
        populateCollection(TEST_DATABASE, TEST_COLLECTION2, INIT_DOCUMENT_COUNT);

        // account has no access to items1 at all, not just a config-level exclusion
        var config = connectorConfiguration(COLLECTION_TWO_USER, COLLECTION_TWO_PWD, TEST_DATABASE + "." + TEST_COLLECTION2);

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(10);
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic1)).isNull();
        AssertionsForClassTypes.assertThat(logInterceptor.containsMessage("Change stream is restricted to '" + TEST_COLLECTION2 + "' collection")).isTrue();
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic2).size()).isEqualTo(INIT_DOCUMENT_COUNT);
    }

    @Test
    @FixFor("dbz#2716")
    public void shouldFailToValidateWithoutPrivilegeOnTargetCollection() {
        var logInterceptor = new LogInterceptor(MongoDbConnector.class);

        // items1 and items2 both exist; the account only has privilege on items2
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);
        populateCollection(TEST_DATABASE, TEST_COLLECTION2, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration(COLLECTION_TWO_USER, COLLECTION_TWO_PWD, TEST_DATABASE + "." + TEST_COLLECTION1);

        start(MongoDbConnector.class, config);

        // Connector should fail immediately during config validation, before any task is ever started
        Awaitility.await().pollDelay(10, TimeUnit.SECONDS).timeout(30, TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        AssertionsForClassTypes.assertThat(logInterceptor.containsMessage("User doesn't have sufficient privileges")).isTrue();
    }

    @Test
    @FixFor("dbz#2716")
    public void shouldValidateSuccessfullyWhenCollectionDoesNotYetExist() {
        var logInterceptor = new LogInterceptor(MongoDbConnector.class);

        // dbit physically exists (items1 has data), but items2, the actual target, doesn't yet.
        // The account's privilege genuinely matches the configured target, it's just empty so far.
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration(COLLECTION_TWO_USER, COLLECTION_TWO_PWD, TEST_DATABASE + "." + TEST_COLLECTION2);

        start(MongoDbConnector.class, config);

        // Connector should validate successfully and keep running even though items2 doesn't exist yet
        Awaitility.await().pollDelay(10, TimeUnit.SECONDS).timeout(30, TimeUnit.SECONDS).until(() -> isEngineRunning.get());
        AssertionsForClassTypes.assertThat(logInterceptor.containsMessage("Could not validate connector config")).isFalse();
    }

    @Test
    @FixFor("dbz#2716")
    public void shouldValidateSuccessfullyWhenDatabaseDoesNotYetExist() {
        var logInterceptor = new LogInterceptor(MongoDbConnector.class);

        // only items1 in dbit exists; dbother/itemsOther are never populated anywhere in this test.
        // The account's privilege genuinely matches the configured target, it's just that neither
        // the database nor the collection have been written to yet.
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration(OTHER_DATABASE_USER, OTHER_DATABASE_PWD, OTHER_DATABASE + "." + OTHER_DATABASE_COLLECTION);

        start(MongoDbConnector.class, config);

        // Connector should validate successfully and keep running even though dbother doesn't exist yet
        Awaitility.await().pollDelay(10, TimeUnit.SECONDS).timeout(30, TimeUnit.SECONDS).until(() -> isEngineRunning.get());
        AssertionsForClassTypes.assertThat(logInterceptor.containsMessage("Could not validate connector config")).isFalse();
    }

    @Test
    @FixFor("DBZ-7760")
    public void shouldConsumeEventsFromSingleCollection() throws InterruptedException {
        var topic = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION1);

        // populate collection
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration(TEST_USER, TEST_PWD, TEST_DATABASE + "." + TEST_COLLECTION1);

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(10);
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic).size()).isEqualTo(INIT_DOCUMENT_COUNT);
    }

    @Test
    @FixFor("DBZ-7760")
    public void shouldNotConsumeEventsFromCollectionWithoutScope() throws InterruptedException {
        LogInterceptor logInterceptor = new LogInterceptor(MongoUtils.class);
        var topic1 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION1);
        var topic2 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION2);

        // populate collection
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);
        populateCollection(TEST_DATABASE, TEST_COLLECTION2, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration(TEST_USER, TEST_PWD, TEST_DATABASE + "." + TEST_COLLECTION2);

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(10);
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic1)).isNull();
        AssertionsForClassTypes.assertThat(logInterceptor.containsMessage("Change stream is restricted to '" + TEST_COLLECTION2 + "' collection")).isTrue();
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic2).size()).isEqualTo(INIT_DOCUMENT_COUNT);
    }
}
