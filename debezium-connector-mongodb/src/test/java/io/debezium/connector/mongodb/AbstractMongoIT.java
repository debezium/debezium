/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.junit.MongoDbDatabaseProvider;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.util.Testing;

public abstract class AbstractMongoIT {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractMongoIT.class);
    protected static MongoDbDeployment mongo;

    protected Configuration config;
    protected MongoDbTaskContext context;
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
        return MongoClients.create(mongo.getConnectionString());
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
        initialize(true);
    }

    /**
     * A method that will initialize the state after the configuration is changed, reusing the same partition offsets that
     * were previously used.
     *
     * @param config the configuration; may not be null
     */
    protected void reuseConfiguration(Configuration config) {
        this.config = config;
        initialize(false);
    }

    /**
     * A method that will initialize the state after the configuration is changed.
     *
     * @param restartFromBeginning {@code true} if the context should have no prior partition offsets, or {@code false} if the
     *            partition offsets that exist at this time should be reused
     */
    private void initialize(boolean restartFromBeginning) {
        // Record the partition offsets (if there are some) ...
        Map<String, ?> offsetForPartition = null;
        var rsName = ConnectionStrings.replicaSetName(mongo.getConnectionString());
        if (!restartFromBeginning && context != null && mongo != null && context.source().hasOffset()) {
            offsetForPartition = context.source().lastOffset();
        }

        context = new MongoDbTaskContext(config);
        assertThat(context.getConnectionContext().connectionSeed()).isNotEmpty();

        // Restore Source position (if there are some) ...
        if (offsetForPartition != null) {
            context.source().setOffset(offsetForPartition);
        }

        // Get a connection to the primary ...

        var connectionString = new ConnectionString(TestHelper.connectionString(mongo));
        connection = context.getConnectionContext().connect(connectionString, context.filters(), TestHelper.connectionErrorHandler(3));
    }
}
