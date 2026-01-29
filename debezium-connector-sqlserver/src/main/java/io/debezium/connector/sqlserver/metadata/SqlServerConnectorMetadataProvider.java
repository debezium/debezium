/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metadata;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

public class SqlServerConnectorMetadataProvider implements ComponentMetadataProvider {
    @Override
    public ComponentMetadata getConnectorMetadata() {
        return new SqlServerConnectorMetadata();
    }
}
