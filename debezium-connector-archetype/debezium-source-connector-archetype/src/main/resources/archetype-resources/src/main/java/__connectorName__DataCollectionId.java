/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.List;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Identifies a single data collection (table/topic) monitored by the ${connectorName} connector.
 *
 * <p>Implement this class to represent the logical identifier for your data source's
 * collection (e.g. a table name, file path, collection name). The {@link #identifier()}
 * value is used in topic naming and offset storage.
 */
public class ${connectorName}DataCollectionId implements DataCollectionId {

    private final String name;

    public ${connectorName}DataCollectionId(String name) {
        this.name = name;
    }

    @Override
    public String identifier() {
        return name;
    }

    @Override
    public List<String> parts() {
        return List.of(name);
    }

    @Override
    public List<String> databaseParts() {
        return List.of();
    }

    @Override
    public List<String> schemaParts() {
        return List.of(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
