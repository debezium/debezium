/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseVersionResolver;
import io.debezium.connector.mongodb.junit.MongoDbPlatform;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;

/**
 * @author Anisha Mohanty
 */
public class FiltersRestrictedIT extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FiltersRestrictedIT.class);
    public static final String AUTH_DATABASE = "admin";
    public static final String TEST_DATABASE1 = "dbit1";
    public static final String TEST_DATABASE2 = "dbit2";
    public static final String TEST_COLLECTION1 = "collection1";
    public static final String TEST_COLLECTION2 = "collection2";
    public static final String TEST_USER = "testUser";
    public static final String TEST_PWD = "testPassword";
    public static final String TOPIC_PREFIX = "mongo";
    private static final int INIT_DOCUMENT_COUNT = 10;
    protected static MongoDbReplicaSet mongo;

    @BeforeClass
    public static void beforeAll() {
        Assume.assumeTrue(MongoDbDatabaseVersionResolver.getPlatform().equals(MongoDbPlatform.MONGODB_DOCKER));
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.dockerAuthReplicaSet();
        LOGGER.info("Starting {}...", mongo);
        mongo.start();
        LOGGER.info("Setting up users");
        mongo.createUser(TEST_USER, TEST_PWD, AUTH_DATABASE, "read:" + TEST_DATABASE1);
    }

    @AfterClass
    public static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            LOGGER.info("Stopping {}...", mongo);
            mongo.stop();
        }
    }

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        cleanDatabase(mongo, TEST_DATABASE1);
        cleanDatabase(mongo, TEST_DATABASE2);
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

    protected Configuration connectorConfiguration() {
        var connectionString = mongo.getAuthConnectionString(TEST_USER, TEST_PWD, AUTH_DATABASE);
        return TestHelper.getConfiguration(connectionString).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(MongoDbConnectorConfig.CAPTURE_SCOPE, MongoDbConnectorConfig.CaptureScope.DATABASE)
                .with(MongoDbConnectorConfig.CAPTURE_TARGET, TEST_DATABASE1)
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 2)
                .build();
    }

    @Test
    @FixFor("DBZ-7485")
    public void shouldNotConsumeEventsFromRestrictedDatabaseCollection() throws InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(MongoDbSchema.class);
        var topic1 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE1, TEST_COLLECTION1);
        var topic2 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE2, TEST_COLLECTION1);

        // populate collection
        populateCollection(TEST_DATABASE1, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);
        populateCollection(TEST_DATABASE2, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);

        // modify the configuration to include database and collection filters
        // include database dbit1 and collection dbit2.collection1
        var config = connectorConfiguration().edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, TEST_DATABASE1)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, TEST_DATABASE2 + "." + TEST_COLLECTION1)
                .build();

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(20);
        assertThat(records.recordsForTopic(topic1)).isNull();
        assertThat(records.recordsForTopic(topic2)).isNull();

        logInterceptor.containsWarnMessage("After applying the include/exclude list filters, no changes will be captured. Please check your configuration!");
    }

    @Test
    @FixFor("DBZ-7485")
    public void shouldConsumeEventsOnlyOnCollectionLevelNotDatabaseLevel() throws InterruptedException {
        var topic1 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE1, TEST_COLLECTION1);
        var topic2 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE1, TEST_COLLECTION2);

        // populate collection
        populateCollection(TEST_DATABASE1, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);
        populateCollection(TEST_DATABASE1, TEST_COLLECTION2, INIT_DOCUMENT_COUNT);

        // modify the configuration to include database and collection filters
        // include database dbit1 and only collection dbit1.collection1
        var config = connectorConfiguration().edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, TEST_DATABASE1)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, TEST_DATABASE1 + "." + TEST_COLLECTION1)
                .build();

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(20);
        assertThat(records.recordsForTopic(topic1).size()).isEqualTo(INIT_DOCUMENT_COUNT);
        assertThat(records.recordsForTopic(topic2)).isNull();
        assertThat(records.topics().size()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-7485")
    public void shouldConsumeEventsOnlyFromIncludedDatabaseCollections() throws InterruptedException {
        var topic1 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE1, TEST_COLLECTION1);
        var topic2 = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE1, TEST_COLLECTION2);

        // populate collection
        populateCollection(TEST_DATABASE1, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);
        populateCollection(TEST_DATABASE1, TEST_COLLECTION2, INIT_DOCUMENT_COUNT);

        // modify the configuration to include database and collection filters
        // include database dbit1 and exclude collection dbit1.collection2

        var config = connectorConfiguration().edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, TEST_DATABASE1)
                .with(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST, TEST_DATABASE1 + "." + TEST_COLLECTION2)
                .build();

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(20);
        assertThat(records.recordsForTopic(topic1).size()).isEqualTo(INIT_DOCUMENT_COUNT);
        assertThat(records.recordsForTopic(topic2)).isNull();
        assertThat(records.topics().size()).isEqualTo(1);
    }
}
