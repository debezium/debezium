/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.mongodb.AbstractShardedMongoConnectorIT;
import io.debezium.connector.mongodb.sink.junit.NetworkIsolatedMongoDbDatabaseProvider;
import io.debezium.junit.RequiresAssemblyProfile;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.MongoDbShardedCluster;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.debezium.testing.testcontainers.util.DockerUtils;

@RequiresAssemblyProfile
public class SinkConnectorShardedClusterIT extends AbstractShardedMongoConnectorIT implements SinkConnectorIT {

    protected static MongoDbDeployment mongo;

    @Override
    public MongoDbDeployment getMongoDbDeployment() {
        return mongo;
    }

    @BeforeAll
    static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = new NetworkIsolatedMongoDbDatabaseProvider(TestInfrastructureHelper.getNetwork()).mongoDbShardedCluster();
        mongo.start();
    }

    @BeforeEach
    public void beforeEach() {
        sendSourceData();

        var database = shardedDatabase();
        var shardedCluster = (MongoDbShardedCluster) mongo;
        shardedCluster.enableSharding(database);
        shardedCollections().forEach((collection, key) -> {
            shardedCluster.shardCollection(database, collection, key);
        });
    }

    @AfterEach
    public void afterEach() {
        cleanDatabase(mongo, DATABASE_NAME);
    }

    @AfterAll
    static void afterAll() {
        SinkConnectorIT.stopContainers(mongo);
        DockerUtils.disableFakeDns();
    }

    @Test
    void testSinkConnectorWritesRecordsToShardedCluster() {
        checkSinkConnectorWritesRecords();
    }
}
