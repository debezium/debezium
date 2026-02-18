/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metadata;

import java.util.List;

import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all MongoDB connector and transformation metadata.
 */
public class MongoDbMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new MongoDbConnectorMetadata(),
                new MongoDbSinkConnectorMetadata(),
                componentMetadataFactory.createComponentMetadata(new ExtractNewDocumentState<>(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new MongoEventRouter<>(), Module.version()));
    }
}
