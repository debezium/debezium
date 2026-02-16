/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter;
import io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverterConfig;
import io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter;
import io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverterConfig;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.transforms.ReadToInsertEvent;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ComponentMetadataUtils;

/**
 * Aggregator for all MySQL connector, transformation, and custom converter metadata.
 */
public class MySqlMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new MySqlConnectorMetadata(),
                createComponentMetadata(ReadToInsertEvent.class, ReadToInsertEvent.class),
                createComponentMetadata(TinyIntOneToBooleanConverter.class, TinyIntOneToBooleanConverterConfig.class),
                createComponentMetadata(JdbcSinkDataTypesConverter.class, JdbcSinkDataTypesConverterConfig.class));
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
