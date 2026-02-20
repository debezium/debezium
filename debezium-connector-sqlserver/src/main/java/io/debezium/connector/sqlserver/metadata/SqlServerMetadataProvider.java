/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metadata;

import java.util.List;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all SQL Server connector metadata.
 */
public class SqlServerMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(new SqlServerConnectorMetadata());
    }
}
