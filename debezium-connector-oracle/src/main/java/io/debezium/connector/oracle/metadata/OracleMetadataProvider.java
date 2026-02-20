/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.metadata;

import java.util.List;

import io.debezium.config.Field;
import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.converters.NumberOneToBooleanConverter;
import io.debezium.connector.oracle.converters.NumberToZeroScaleConverter;
import io.debezium.connector.oracle.converters.RawToStringConverter;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ConfigDescriptor;

/**
 * Aggregator for all Oracle connector and custom converter metadata.
 */
public class OracleMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new OracleConnectorMetadata(),
                createComponentMetadata(new NumberToZeroScaleConverter()),
                createComponentMetadata(new RawToStringConverter()),
                createComponentMetadata(new NumberOneToBooleanConverter()));
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
