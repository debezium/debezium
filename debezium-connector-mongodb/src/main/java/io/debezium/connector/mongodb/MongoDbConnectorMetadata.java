/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Arrays;
import java.util.List;

import io.debezium.config.Field;
import io.debezium.metadata.AbstractConnectorMetadata;
import io.debezium.metadata.ConnectorDescriptor;

public class MongoDbConnectorMetadata extends AbstractConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("mongodb", "Debezium MongoDB Connector", MongoDbConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return MongoDbConnectorConfig.ALL_FIELDS;
    }

    @Override
    public List<String> deprecatedFieldNames() {
        return Arrays.asList(
                MongoDbConnectorConfig.POLL_INTERVAL_SEC.name(),
                MongoDbConnectorConfig.MAX_COPY_THREADS.name());
    }

}
