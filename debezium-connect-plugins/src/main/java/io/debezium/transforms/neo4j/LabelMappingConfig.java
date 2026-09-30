/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.config.ConfigException;

/**
 * Immutable optional override mapping for a single Neo4j node label.
 * Every field may be absent (empty); the {@code DebeziumEnvelopeFactory} falls back to the convention for anything not overridden here.
 */
public record LabelMappingConfig(String label, String table, List<String> keyProperties,
        Set<String> propertiesInclude, Set<String> propertiesExclude) {

    public LabelMappingConfig {
        if (!propertiesInclude.isEmpty() && !propertiesExclude.isEmpty()) {
            throw new ConfigException("label." + label + ".properties.include and label." + label
                    + ".properties.exclude are mutually exclusive");
        }
    }
}
