/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.converters.BinaryDataConverter;
import io.debezium.converters.ByteArrayConverter;
import io.debezium.converters.CloudEventsConverter;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.Module;

/**
 * Aggregator for all Debezium converters metadata.
 */
public class ConverterMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                createComponentMetadata(new BinaryDataConverter()),
                createComponentMetadata(new ByteArrayConverter()),
                createComponentMetadata(new CloudEventsConverter()));
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
