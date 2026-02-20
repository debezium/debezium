/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;
import io.debezium.connector.mongodb.transforms.outbox.MongoEventRouter;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ComponentMetadataUtils;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import io.debezium.transforms.outbox.EventRouterConfigDefinition;
import io.debezium.transforms.tracing.ActivateTracingSpan;

/**
 * Aggregator for all MongoDB connector and transformation metadata.
 */
public class MongoDbMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new MongoDbConnectorMetadata(),
                new MongoDbSinkConnectorMetadata(),
                createComponentMetadata(
                        ExtractNewDocumentState.class,
                        ExtractNewDocumentState.class,
                        ExtractNewRecordStateConfigDefinition.class),
                createComponentMetadata(
                        MongoEventRouter.class,
                        EventRouterConfigDefinition.class,
                        ActivateTracingSpan.class));
    }

    private ComponentMetadata createComponentMetadata(Class<?> componentClass, Class<?>... configClasses) {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(componentClass.getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return ComponentMetadataUtils.extractFieldConstants(configClasses);
            }
        };
    }
}
