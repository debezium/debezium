/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.config.Field;
import io.debezium.metadata.AbstractConnectorMetadata;
import io.debezium.metadata.ConnectorDescriptor;

public class PostgresConnectorMetadata extends AbstractConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", getConnector().version());
    }

    @Override
    public Connector getConnector() {
        return new PostgresConnector();
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

}
