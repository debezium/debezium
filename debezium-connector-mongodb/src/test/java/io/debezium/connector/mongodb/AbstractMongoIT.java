/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.MongoDbConnections;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.util.Testing;

public abstract class AbstractMongoIT {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractMongoIT.class);
    protected static MongoDbDeployment mongo;

    protected Configuration config;
    protected MongoDbConnection connection;

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
        return TestHelper.connect(mongo);
    }

    @Before
    public void beforeEach() {
        Testing.Print.disable();
        Testing.Debug.disable();
        useConfiguration(TestHelper.getConfiguration(mongo));
    }

    /**
     * A method that will initialize the state after the configuration is changed.
     *
     * @param config the configuration; may not be null
     */
    protected void useConfiguration(Configuration config) {
        this.config = config;
        initialize();
    }

    /**
     * A method that will initialize the state after the configuration is changed.
     */
    private void initialize() {
        connection = MongoDbConnections.create(config, TestHelper.connectionErrorHandler(3));
    }
}
