/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metadata;

import io.debezium.metadata.ConnectorMetadata;
import io.debezium.metadata.ConnectorMetadataProvider;

public class PostgresConnectorMetadataProvider implements ConnectorMetadataProvider {

    @Override
    public ConnectorMetadata getConnectorMetadata() {
        return new PostgresConnectorMetadata();
    }
}
