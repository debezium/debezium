/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.MongoDbReplicaSet.replicaSet;

import java.util.ArrayList;

import io.debezium.junit.Flaky;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClients;

import io.debezium.junit.Flaky;
import io.debezium.testing.testcontainers.util.DockerUtils;

/**
 * @see <a href="https://issues.redhat.com/browse/DBZ-5857">DBZ-5857</a>
 */
@Flaky("DBZ-7507")
public class MongoDbReplicaSetAuthTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbReplicaSetAuthTest.class);

    public static final String AUTH_DATABASE = "admin";
    public static final String TEST_DATABASE_1 = "fstDB";
    public static final String TEST_USER = "testUser";
    public static final String TEST_PWD = "testSecret";
    public static final String TEST_DATABASE_2 = "sndDB";
    public static final String TEST_COLLECTION = "examples";

    private static MongoDbReplicaSet mongo;

    @BeforeAll
    static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = replicaSet().authEnabled(true).build();
        LOGGER.info("Starting {}...", mongo);
        mongo.start();
        LOGGER.info("Setting up users");
        mongo.createUser(TEST_USER, TEST_PWD, AUTH_DATABASE, "read:" + TEST_DATABASE_1);
    }

    @AfterAll
    static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            mongo.stop();
        }
    }

    @BeforeEach
    void before() {
        setupDatabase(mongo, TEST_DATABASE_1);
        setupDatabase(mongo, TEST_DATABASE_2);
    }

    void setupDatabase(MongoDbDeployment mongo, String dbName) {
        try (var client = MongoClients.create(mongo.getConnectionString())) {
            LOGGER.info("Connected to cluster: {}", client.getClusterDescription());
            var database = client.getDatabase(dbName);
            database.drop();

            var collection = database.getCollection(TEST_COLLECTION);
            collection.insertOne(Document.parse("{example: true}"));
        }
    }

    @Test
    @Flaky("DBZ-7507")
    public void testCluster() {
        var noAuthConnectionString = mongo.getNoAuthConnectionString();
        LOGGER.info("Connecting to cluster without credentials: {}", noAuthConnectionString);
        try (var client = MongoClients.create(noAuthConnectionString)) {
            LOGGER.info("Connected to cluster: {}", client.getClusterDescription());
            client.getDatabase(TEST_DATABASE_1).listCollectionNames();
        }
        catch (MongoCommandException e) {
            Assertions.assertThat(e.getMessage()).contains("Unauthorized");
        }

        var connectionString = mongo.getConnectionString();
        LOGGER.info("Connecting to cluster as root: {}", connectionString);
        try (var client = MongoClients.create(connectionString)) {
            LOGGER.info("Connected to cluster: {}", client.getClusterDescription());
            client.getDatabase(TEST_DATABASE_1).listCollectionNames();
        }

        var authConnectionString = mongo.getAuthConnectionString(TEST_USER, TEST_PWD, AUTH_DATABASE);
        LOGGER.info("Connecting to cluster as {}: {}", TEST_USER, authConnectionString);
        try (var client = MongoClients.create(authConnectionString)) {
            // TEST_USER can read TEST_DATABASE_1
            var names = new ArrayList<String>();
            client.getDatabase(TEST_DATABASE_1).listCollectionNames().into(names);
            Assertions.assertThat(names).containsOnly(TEST_COLLECTION);
        }
        try (var client = MongoClients.create(authConnectionString)) {
            // TEST_USER can NOT read TEST_DATABASE_2
            client.getDatabase(TEST_DATABASE_2).listCollectionNames().first();
        }
        catch (MongoCommandException e) {
            Assertions.assertThat(e.getMessage()).contains("not authorized");
        }
    }
}
