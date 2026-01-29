/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.jdbc.JdbcSinkConnector;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.Module;
import io.debezium.connector.jdbc.transforms.CollectionNameTransformation;
import io.debezium.connector.jdbc.transforms.FieldNameTransformation;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ComponentMetadataUtils;

/**
 * Aggregator for all JDBC connector and transformation metadata.
 */
public class JdbcMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                createSinkConnectorMetadata(),
                createTransformMetadata(CollectionNameTransformation.class),
                createTransformMetadata(FieldNameTransformation.class));
    }

    private ComponentMetadata createSinkConnectorMetadata() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(JdbcSinkConnector.class.getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return JdbcSinkConnectorConfig.ALL_FIELDS;
            }
        };
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
