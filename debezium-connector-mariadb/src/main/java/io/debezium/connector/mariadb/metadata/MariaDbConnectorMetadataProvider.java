/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metadata;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * @author Chris Cranford
 */
public class MariaDbConnectorMetadataProvider implements ComponentMetadataProvider {
    @Override
    public ComponentMetadata getConnectorMetadata() {
        return new MariaDbConnectorMetadata();
    }
}
