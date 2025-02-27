/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metadata;

import io.debezium.config.Field;
import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class SqlServerConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor(SqlServerConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return SqlServerConnectorConfig.ALL_FIELDS;
    }
}
