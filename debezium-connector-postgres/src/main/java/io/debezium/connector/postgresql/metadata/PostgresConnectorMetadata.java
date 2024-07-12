/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metadata;

import io.debezium.config.Field;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.YugabyteDBConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class PostgresConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", YugabyteDBConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

}
