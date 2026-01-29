/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metadata;

import io.debezium.config.Field;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbSinkConnector;
import io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;

public class MongoDbSinkConnectorMetadata implements ComponentMetadata {

    @Override
    public ComponentDescriptor getComponentDescriptor() {
        return new ComponentDescriptor(MongoDbSinkConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getComponentFields() {
        return MongoDbSinkConnectorConfig.ALL_FIELDS;
    }
}
