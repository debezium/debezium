/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ReadPreference;

import io.debezium.config.Configuration;
import io.debezium.util.Testing;

public abstract class AbstractMongoIT extends AbstractBaseMongoIT {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractMongoIT.class);

    protected Configuration config;
    protected MongoDbTaskContext context;
    protected ReplicaSet replicaSet;
    protected RetryingMongoClient primary;

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
        Map<String, String> partition = null;
        Map<String, ?> offsetForPartition = null;
        if (!restartFromBeginning && context != null && replicaSet != null && context.source().hasOffset(replicaSet.replicaSetName())) {
            partition = context.source().partition(replicaSet.replicaSetName());
            offsetForPartition = context.source().lastOffset(replicaSet.replicaSetName());
        }

        context = new MongoDbTaskContext(config);
        assertThat(context.getConnectionContext().connectionSeed()).isNotEmpty();

        replicaSet = new ReplicaSet(mongo.getConnectionString());
        context.configureLoggingContext(replicaSet.replicaSetName());

        // Restore Source position (if there are some) ...
        if (partition != null) {
            context.source().setOffsetFor(partition, offsetForPartition);
        }

        // Get a connection to the primary ...
        primary = context.getConnectionContext().connect(
                replicaSet, ReadPreference.primary(), context.filters(), TestHelper.connectionErrorHandler(3));
    }

    @After
    public void afterEach() {
        if (context != null) {
            // close all connections
            context.getConnectionContext().close();
        }
    }
}
