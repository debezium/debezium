/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.Configuration;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.NodeMode;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.OutputMode;

class Neo4jCudConfigParser {

    private static final String RELATIONSHIP_PREFIX = "relationship.";

    static Neo4jCudConverterConfig parse(Configuration config, Map<String, ?> rawProps) {
        final var nodeMode = NodeMode.parse(config.getString(Neo4jCudConverterConfig.NODE_MODE));
        final var nodeLabels = parseCommaSeparated(config.getString(Neo4jCudConverterConfig.NODE_LABELS));
        final var nodeIdProperties = parseCommaSeparated(config.getString(Neo4jCudConverterConfig.NODE_ID_PROPERTIES));
        final var propertiesInclude = toSet(parseCommaSeparated(config.getString(Neo4jCudConverterConfig.NODE_PROPERTIES_INCLUDE)));
        final var propertiesExclude = toSet(parseCommaSeparated(config.getString(Neo4jCudConverterConfig.NODE_PROPERTIES_EXCLUDE)));
        final var deleteDetach = config.getBoolean(Neo4jCudConverterConfig.NODE_DELETE_DETACH);
        final var outputMode = OutputMode.parse(config.getString(Neo4jCudConverterConfig.OUTPUT_MODE));
        final var tombstonesEnabled = config.getBoolean(Neo4jCudConverterConfig.TOMBSTONES_ENABLED);
        final var relationships = parseRelationships(rawProps);

        return new Neo4jCudConverterConfig(
                nodeMode, nodeLabels, nodeIdProperties,
                propertiesInclude, propertiesExclude, deleteDetach,
                outputMode, tombstonesEnabled, relationships);
    }

    static List<String> parseCommaSeparated(String value) {
        if (value == null || value.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private static Set<String> toSet(List<String> list) {
        if (list.isEmpty()) {
            return Collections.emptySet();
        }
        return Set.copyOf(list);
    }

    private static List<RelationshipMapping> parseRelationships(Map<String, ?> rawProps) {
        final var grouped = groupByFkColumn(rawProps);
        final List<RelationshipMapping> mappings = new ArrayList<>();
        for (final var entry : grouped.entrySet()) {
            mappings.add(buildMapping(entry.getKey(), entry.getValue()));
        }
        return Collections.unmodifiableList(mappings);
    }

    private static Map<String, Map<String, String>> groupByFkColumn(Map<String, ?> rawProps) {
        final Map<String, Map<String, String>> grouped = new LinkedHashMap<>();
        for (final var entry : rawProps.entrySet()) {
            final var key = entry.getKey();
            if (!key.startsWith(RELATIONSHIP_PREFIX)) {
                continue;
            }
            final var withoutPrefix = key.substring(RELATIONSHIP_PREFIX.length());
            final var dotIndex = withoutPrefix.indexOf('.');
            if (dotIndex < 0) {
                continue;
            }
            final var fkColumn = withoutPrefix.substring(0, dotIndex);
            final var subKey = withoutPrefix.substring(dotIndex + 1);
            grouped.computeIfAbsent(fkColumn, k -> new LinkedHashMap<>())
                    .put(subKey, String.valueOf(entry.getValue()));
        }
        return grouped;
    }

    private static RelationshipMapping buildMapping(String fkColumn, Map<String, String> subKeys) {
        final var type = requireNonBlank(subKeys.get("type"), RELATIONSHIP_PREFIX + fkColumn + ".type");
        final var targetLabel = requireNonBlank(subKeys.get("target.label"), RELATIONSHIP_PREFIX + fkColumn + ".target.label");

        final var directionStr = subKeys.getOrDefault("direction", "outgoing");
        final var direction = "incoming".equalsIgnoreCase(directionStr)
                ? RelationshipMapping.Direction.INCOMING
                : RelationshipMapping.Direction.OUTGOING;

        final var targetNodeOp = parseTargetNodeOp(
                subKeys.getOrDefault("target.node.op", "match"),
                RELATIONSHIP_PREFIX + fkColumn + ".target.node.op");

        return new RelationshipMapping(
                fkColumn,
                type,
                direction,
                targetLabel,
                subKeys.getOrDefault("target.id", fkColumn),
                targetNodeOp,
                parseCommaSeparated(subKeys.get("properties")));
    }

    private static CudEvent.Operation parseTargetNodeOp(String value, String key) {
        try {
            return CudEvent.Operation.valueOf(value.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new ConfigException(key, value, "Must be 'match' or 'merge'");
        }
    }

    private static String requireNonBlank(String value, String key) {
        if (value == null || value.isBlank()) {
            throw new ConfigException(key, null, "Required");
        }
        return value;
    }
}
