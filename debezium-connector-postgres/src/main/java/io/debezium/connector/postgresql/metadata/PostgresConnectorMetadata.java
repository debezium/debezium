/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metadata;

import io.debezium.config.Field;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.metadata.AbstractConnectorMetadata;
import io.debezium.metadata.ConnectorDescriptor;

public class PostgresConnectorMetadata extends AbstractConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", PostgresConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

}
