/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.metadata;

import java.util.List;

import io.debezium.connector.jdbc.JdbcSinkConnector;
import io.debezium.connector.jdbc.Module;
import io.debezium.connector.jdbc.transforms.CollectionNameTransformation;
import io.debezium.connector.jdbc.transforms.FieldNameTransformation;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all JDBC connector and transformation metadata.
 */
public class JdbcMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new JdbcSinkConnector(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new CollectionNameTransformation<>(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new FieldNameTransformation<>(), Module.version()));
    }

}
