/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.AssertionsForClassTypes;
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
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;

public class MongoDbConnectorCollectionRestrictedIT extends AbstractConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectorCollectionRestrictedIT.class);
    public static final String AUTH_DATABASE = "admin";
    public static final String TEST_DATABASE = "dbit";
    public static final String TEST_USER = "testUser";
    public static final String TEST_PWD = "testSecret";
    public static final String TEST_COLLECTION1 = "items1";
    public static final String TEST_COLLECTION2 = "items2";
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
        mongo.createUser(TEST_USER, TEST_PWD, AUTH_DATABASE, "read:" + TEST_DATABASE);
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

    protected Configuration connectorConfiguration() {
        var connectionString = mongo.getAuthConnectionString(TEST_USER, TEST_PWD, AUTH_DATABASE);
        return TestHelper.getConfiguration(connectionString).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(CommonConnectorConfig.MAX_RETRIES_ON_ERROR, 2)
                .build();
    }

    @Test
    @FixFor("DBZ-7760")
    public void shouldConsumeEventsFromSingleCollection() throws InterruptedException {
        var topic = String.format("%s.%s.%s", TOPIC_PREFIX, TEST_DATABASE, TEST_COLLECTION1);

        // populate collection
        populateCollection(TEST_DATABASE, TEST_COLLECTION1, INIT_DOCUMENT_COUNT);

        var config = connectorConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_SCOPE, MongoDbConnectorConfig.CaptureScope.COLLECTION)
                .with(MongoDbConnectorConfig.CAPTURE_TARGET, TEST_DATABASE + "." + TEST_COLLECTION1)
                .build();

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

        var config = connectorConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_SCOPE, MongoDbConnectorConfig.CaptureScope.COLLECTION)
                .with(MongoDbConnectorConfig.CAPTURE_TARGET, TEST_DATABASE + "." + TEST_COLLECTION2)
                .build();

        // start the connector
        start(MongoDbConnector.class, config);

        // consume documents
        SourceRecords records = consumeRecordsByTopic(10);
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic1)).isNull();
        AssertionsForClassTypes.assertThat(logInterceptor.containsMessage("Change stream is restricted to '" + TEST_COLLECTION2 + "' collection")).isTrue();
        AssertionsForClassTypes.assertThat(records.recordsForTopic(topic2).size()).isEqualTo(INIT_DOCUMENT_COUNT);
    }
}
