/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collections;
import java.util.List;

import io.debezium.config.Field;
import io.debezium.metadata.AbstractConnectorMetadata;
import io.debezium.metadata.ConnectorDescriptor;

public class MySqlConnectorMetadata extends AbstractConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("mysql", "Debezium MySQL Connector", MySqlConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }

    @Override
	public List<String> deprecatedFieldNames() {
        return Collections.singletonList(MySqlConnectorConfig.GTID_NEW_CHANNEL_POSITION.name());
    }
}
