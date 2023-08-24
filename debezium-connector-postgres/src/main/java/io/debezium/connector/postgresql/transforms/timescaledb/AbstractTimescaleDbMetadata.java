/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.util.HashSet;
import java.util.Set;

import io.debezium.config.Configuration;

/**
 *
 * @author Jiri Pechanec
 *
 */
public abstract class AbstractTimescaleDbMetadata implements TimescaleDbMetadata {

    private final Set<String> timescaleDbSchemas;

    protected AbstractTimescaleDbMetadata(Configuration config) {
        timescaleDbSchemas = new HashSet<>(config.getList(TimescaleDbConfigDefinition.SCHEMA_LIST_NAMES_FIELD));
        if (timescaleDbSchemas.isEmpty()) {
            timescaleDbSchemas.add(TimescaleDbConfigDefinition.SCHEMA_LIST_NAMES_FIELD.defaultValueAsString());
        }
    }

    public boolean isTimescaleDbSchema(String schemaName) {
        return timescaleDbSchemas.contains(schemaName);
    }
}
