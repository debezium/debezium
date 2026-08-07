/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.NodeMode;

/**
 * Immutable mapping for a single source table.
 * A {@link Neo4jCudConverterConfig} holds one of these per configured table and dispatches to the
 * matching mapping based on the {@code source.table} of each change event.
 */
public class TableMappingConfig {

    private final String tableName;
    private final NodeMode nodeMode;
    private final List<String> nodeLabels;
    private final List<String> nodeIdProperties;
    private final Set<String> propertiesInclude;
    private final Set<String> propertiesExclude;
    private final boolean deleteDetach;
    private final List<RelationshipMapping> relationships;
    private final Set<String> fkColumns;

    TableMappingConfig(String tableName, NodeMode nodeMode, List<String> nodeLabels, List<String> nodeIdProperties,
                       Set<String> propertiesInclude, Set<String> propertiesExclude, boolean deleteDetach,
                       List<RelationshipMapping> relationships) {
        this.tableName = tableName;
        this.nodeMode = nodeMode;
        this.nodeLabels = nodeLabels;
        this.nodeIdProperties = nodeIdProperties;
        this.propertiesInclude = propertiesInclude;
        this.propertiesExclude = propertiesExclude;
        this.deleteDetach = deleteDetach;
        this.relationships = relationships;
        this.fkColumns = relationships.stream()
                .map(RelationshipMapping::fkColumn)
                .collect(Collectors.toSet());

        validate();
    }

    private void validate() {
        final var prefix = "table." + tableName + ".";
        if (nodeMode == NodeMode.NODE && nodeIdProperties.isEmpty()) {
            throw new ConfigException(prefix + "node.id.properties", null, "Required when node.mode=node");
        }
        if (nodeMode == NodeMode.NODE && nodeLabels.isEmpty()) {
            throw new ConfigException(prefix + "node.labels", null, "Required when node.mode=node");
        }
        if (nodeMode == NodeMode.RELATIONSHIP) {
            if (relationships.size() != 2) {
                throw new ConfigException(prefix + "relationship.*", relationships.size(),
                        "node.mode=relationship requires exactly two relationship mappings (a source and a target endpoint)");
            }
            final var outgoing = relationships.stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.OUTGOING).count();
            final var incoming = relationships.stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.INCOMING).count();
            if (outgoing != 1 || incoming != 1) {
                throw new ConfigException(prefix + "relationship.<fk_column>.direction", null,
                        "node.mode=relationship requires exactly one outgoing (source) and one incoming (target) relationship endpoint");
            }
        }
        if (!propertiesInclude.isEmpty() && !propertiesExclude.isEmpty()) {
            throw new ConfigException(prefix + "node.properties.include and " + prefix
                    + "node.properties.exclude are mutually exclusive");
        }
    }

    public String tableName() {
        return tableName;
    }

    public NodeMode nodeMode() {
        return nodeMode;
    }

    public List<String> nodeLabels() {
        return nodeLabels;
    }

    public List<String> nodeIdProperties() {
        return nodeIdProperties;
    }

    public Set<String> propertiesInclude() {
        return propertiesInclude;
    }

    public Set<String> propertiesExclude() {
        return propertiesExclude;
    }

    public boolean deleteDetach() {
        return deleteDetach;
    }

    public List<RelationshipMapping> relationships() {
        return relationships;
    }

    public Set<String> fkColumns() {
        return fkColumns;
    }
}
