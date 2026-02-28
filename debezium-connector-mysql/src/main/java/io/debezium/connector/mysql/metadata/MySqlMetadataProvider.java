/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.metadata;

import java.util.List;

import io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter;
import io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter;
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.transforms.ReadToInsertEvent;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all MySQL connector, transformation, and custom converter metadata.
 */
public class MySqlMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new MySqlConnector(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new ReadToInsertEvent<>(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new TinyIntOneToBooleanConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new JdbcSinkDataTypesConverter(), Module.version()));
    }
}
