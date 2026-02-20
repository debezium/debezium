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
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ConfigDescriptor;

/**
 * Aggregator for all PostgreSQL connector and transformation metadata.
 */
public class PostgresMetadataProvider implements ComponentMetadataProvider {

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new PostgresConnectorMetadata(),
                createComponentMetadata(new DecodeLogicalDecodingMessageContent<>()),
                createComponentMetadata(new TimescaleDb<>()));
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
