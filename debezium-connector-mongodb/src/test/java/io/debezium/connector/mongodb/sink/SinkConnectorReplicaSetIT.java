/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.TestHelper.cleanDatabase;
import static org.hamcrest.CoreMatchers.is;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.mongodb.AbstractMongoConnectorIT;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.debezium.testing.testcontainers.util.DockerUtils;

public class SinkConnectorReplicaSetIT extends AbstractMongoConnectorIT implements SinkConnectorIT {

    protected static MongoDbDeployment mongo;

    @Override
    public MongoDbDeployment getMongoDbDeployment() {
        return mongo;
    }

    @BeforeClass
    public static void beforeAll() {
        Assume.assumeThat("Skipping DebeziumMongoDbConnectorResourceIT tests when assembly profile is not active!",
                System.getProperty("isAssemblyProfileActive", "false"),
                is("true"));
        DockerUtils.enableFakeDnsIfRequired();
        mongo = MongoDbDatabaseProvider.externalOrDockerReplicaSet(TestInfrastructureHelper.getNetwork());
        mongo.start();
    }

    @Before
    public void beforeEach() {
        sendSourceData();
    }

    @After
    public void afterEach() {
        cleanDatabase(mongo, DATABASE_NAME);
    }

    @AfterClass
    public static void afterAll() {
        SinkConnectorIT.stopContainers(mongo);
        DockerUtils.disableFakeDns();
    }

    @Test
    public void testSinkConnectorWritesRecordsToReplicaSet() {
        checkSinkConnectorWritesRecords();
    }

}
