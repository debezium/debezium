/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.metadata;

import io.debezium.config.Field;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;

public class MySqlConnectorMetadata implements ComponentMetadata {

    @Override
    public ComponentDescriptor getComponentDescriptor() {
        return new ComponentDescriptor(MySqlConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getComponentFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }
}
