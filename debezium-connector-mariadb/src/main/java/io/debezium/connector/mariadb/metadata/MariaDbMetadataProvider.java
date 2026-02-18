/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metadata;

import java.util.List;

import io.debezium.connector.binlog.converters.JdbcSinkDataTypesConverter;
import io.debezium.connector.binlog.converters.TinyIntOneToBooleanConverter;
import io.debezium.connector.mariadb.Module;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all MariaDB connector and custom converter metadata.
 */
public class MariaDbMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                new MariaDbConnectorMetadata(),
                componentMetadataFactory.createComponentMetadata(new TinyIntOneToBooleanConverter(), Module.version()),
                componentMetadataFactory.createComponentMetadata(new JdbcSinkDataTypesConverter(), Module.version()));
    }
}
