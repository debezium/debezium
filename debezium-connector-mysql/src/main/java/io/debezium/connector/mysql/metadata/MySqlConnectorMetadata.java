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
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class MySqlConnectorMetadata implements ConnectorMetadata {

    /**
     * Defines a variant of SERVER_ID where we override the default connector behavior to only
     * expose it as required with no default value and no default value generator.
     */
    private static Field SERVER_ID = MySqlConnectorConfig.SERVER_ID
            .withNoDefault()
            .withDescription("A numeric ID of this database client, which must be unique across all "
                    + "currently running database processes in the cluster. This connector joins the "
                    + "MySQL database cluster as another server (with this unique ID) so it can read "
                    + "the binlog.");

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("mysql", "Debezium MySQL Connector", MySqlConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return MySqlConnectorConfig.ALL_FIELDS
                .filtered(f -> f != MySqlConnectorConfig.GTID_NEW_CHANNEL_POSITION)
                .filtered(f -> f != MySqlConnectorConfig.SERVER_ID)
                .with(SERVER_ID);
    }
}
