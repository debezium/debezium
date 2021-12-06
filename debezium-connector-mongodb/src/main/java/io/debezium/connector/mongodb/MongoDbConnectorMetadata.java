/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.config.Field;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class MongoDbConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("mongodb", "Debezium MongoDB Connector", MongoDbConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return MongoDbConnectorConfig.ALL_FIELDS
                .filtered(f -> f != MongoDbConnectorConfig.POLL_INTERVAL_SEC && f != MongoDbConnectorConfig.MAX_COPY_THREADS);
    }
}
