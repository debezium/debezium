/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metadata;

import java.util.List;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.transforms.DecodeLogicalDecodingMessageContent;
import io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all PostgreSQL connector and transformation metadata.
 */
public class PostgresMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new PostgresConnector(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new DecodeLogicalDecodingMessageContent<>(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new TimescaleDb<>(), Module.version()));
    }
}
