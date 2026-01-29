/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent;
import io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb;
import io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDbConfigDefinition;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ComponentMetadataUtils;

/**
 * Aggregator for all PostgreSQL connector and transformation metadata.
 */
public class PostgresMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new PostgresConnectorMetadata(),
                createTransformMetadata(
                        DecodeLogicalDecodingMessageContent.class,
                        DecodeLogicalDecodingMessageContent.class),
                createTransformMetadata(
                        TimescaleDb.class,
                        TimescaleDbConfigDefinition.class));
    }

    private ComponentMetadata createTransformMetadata(Class<?> transformClass, Class<?>... configClasses) {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(transformClass.getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return ComponentMetadataUtils.extractFieldConstants(configClasses);
            }
        };
    }
}
