/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.util.Testing;

public class AbstractBaseMongoIT implements Testing {

    protected static MongoDbDeployment mongo;

    @BeforeClass
    public static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.externalOrDockerReplicaSet();
        mongo.start();
    }

    @AfterClass
    public static void afterAll() {
        DockerUtils.disableFakeDns();
        if (mongo != null) {
            mongo.stop();
        }
    }

    protected MongoClient connect() {
        return MongoClients.create(mongo.getConnectionString());
    }
}
