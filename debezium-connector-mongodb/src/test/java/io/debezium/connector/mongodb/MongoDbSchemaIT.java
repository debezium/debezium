/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.schema.DataCollectionSchema;

/**
 * @author Chris Cranford
 */
public class MongoDbSchemaIT {

    private Configuration config;
    private MongoDbTaskContext taskContext;

    @After
    public void afterEach() {
        if (taskContext != null) {
            taskContext.getConnectionContext().shutdown();
        }
    }

    @Test
    public void shouldAlwaysProduceCollectionSchema() {
        config = TestHelper.getConfiguration();
        taskContext = new MongoDbTaskContext(config);

        final MongoDbSchema schema = getSchema(config, taskContext);
        for (int i = 0; i != 100; ++i) {
            CollectionId id = new CollectionId("rs0", "dbA", "c" + i);
            DataCollectionSchema collectionSchema = schema.schemaFor(id);
            assertThat(collectionSchema).isNotNull();
            assertThat(collectionSchema.id()).isSameAs(id);
        }
    }

    private static MongoDbSchema getSchema(Configuration config, MongoDbTaskContext taskContext) {
        final MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);
        return new MongoDbSchema(taskContext.filters(), taskContext.topicNamingStrategy(),
                connectorConfig.getSourceInfoStructMaker().schema(),
                connectorConfig.schemaNameAdjuster());
    }
}
