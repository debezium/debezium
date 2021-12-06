/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Field;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class MySqlConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("mysql", "Debezium MySQL Connector", MySqlConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return MySqlConnectorConfig.ALL_FIELDS
                .filtered(f -> f != MySqlConnectorConfig.GTID_NEW_CHANNEL_POSITION);
    }
}
