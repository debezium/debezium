/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.metadata;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

public class OracleConnectorMetadataProvider implements ComponentMetadataProvider {

    @Override
    public ComponentMetadata getConnectorMetadata() {
        return new OracleConnectorMetadata();
    }
}
