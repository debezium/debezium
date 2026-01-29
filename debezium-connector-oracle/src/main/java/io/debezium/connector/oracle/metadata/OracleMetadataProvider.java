/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.metadata;

import java.util.List;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all Oracle connector metadata.
 */
public class OracleMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(new OracleConnectorMetadata());
    }
}
