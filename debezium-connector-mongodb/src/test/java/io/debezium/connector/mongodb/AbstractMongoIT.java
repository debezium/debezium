/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.util.Testing;

public abstract class AbstractMongoIT implements Testing {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractMongoIT.class);

    protected Configuration config;
    protected MongoDbTaskContext context;
    protected ReplicaSet replicaSet;
    protected MongoPrimary primary;

    @Before
    public void beforeEach() {
        Testing.Print.disable();
        Testing.Debug.disable();
        useConfiguration(TestHelper.getConfiguration());
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
        assertThat(context.getConnectionContext().hosts()).isNotEmpty();

        replicaSet = ReplicaSet.parse(context.getConnectionContext().hosts());
        context.configureLoggingContext(replicaSet.replicaSetName());

        // Restore Source position (if there are some) ...
        if (partition != null) {
            context.source().setOffsetFor(partition, offsetForPartition);
        }

        // Get a connection to the primary ...
        primary = context.getConnectionContext().primaryFor(replicaSet, connectionErrorHandler(3));
    }

    @After
    public void afterEach() {
        if (context != null) {
            // close all connections
            context.getConnectionContext().shutdown();
        }
    }

    protected BiConsumer<String, Throwable> connectionErrorHandler(int numErrorsBeforeFailing) {
        AtomicInteger attempts = new AtomicInteger();
        return (desc, error) -> {
            if (attempts.incrementAndGet() > numErrorsBeforeFailing) {
                fail("Unable to connect to primary after " + numErrorsBeforeFailing + " errors trying to " + desc + ": " + error);
            }
            logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
        };
    }

}
