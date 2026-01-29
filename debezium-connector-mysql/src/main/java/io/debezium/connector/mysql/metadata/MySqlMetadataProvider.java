/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.transforms.ReadToInsertEvent;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ComponentMetadataUtils;

/**
 * Aggregator for all MySQL connector and transformation metadata.
 */
public class MySqlMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new MySqlConnectorMetadata(),
                createTransformMetadata(ReadToInsertEvent.class));
    }

    private ComponentMetadata createTransformMetadata(Class<?> transformClass) {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(transformClass.getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return ComponentMetadataUtils.extractFieldConstants(transformClass);
            }
        };
    }
}
