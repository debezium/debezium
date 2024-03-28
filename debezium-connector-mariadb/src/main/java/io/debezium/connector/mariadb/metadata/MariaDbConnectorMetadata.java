/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metadata;

import io.debezium.config.Field;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.connector.mariadb.Module;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

/**
 * @author Chris Cranford
 */
public class MariaDbConnectorMetadata implements ConnectorMetadata {
    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor(MariaDbConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return MariaDbConnectorConfig.ALL_FIELDS;
    }
}
