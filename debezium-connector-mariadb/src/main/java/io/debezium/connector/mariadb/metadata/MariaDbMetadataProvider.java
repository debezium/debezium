/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter;
import io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter;
import io.debezium.connector.mariadb.Module;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ConfigDescriptor;

/**
 * Aggregator for all MariaDB connector and custom converter metadata.
 */
public class MariaDbMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new MariaDbConnectorMetadata(),
                createComponentMetadata(new TinyIntOneToBooleanConverter()),
                createComponentMetadata(new JdbcSinkDataTypesConverter()));
    }

    private <T extends ConfigDescriptor> ComponentMetadata createComponentMetadata(T component) {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(component.getClass().getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return component.getConfigFields();
            }
        };
    }
}
