/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mongodb.metadata;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

public class MongoDbConnectorMetadataProvider implements ComponentMetadataProvider {

    @Override
    public ComponentMetadata getConnectorMetadata() {
        return new MongoDbConnectorMetadata();
    }
}
