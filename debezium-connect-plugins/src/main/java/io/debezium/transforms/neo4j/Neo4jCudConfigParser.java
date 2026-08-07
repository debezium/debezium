/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.Configuration;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.NodeMode;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.OutputMode;
import io.debezium.util.Strings;

class Neo4jCudConfigParser {

    private static final String RELATIONSHIP_PREFIX = "relationship.";

    static Neo4jCudConverterConfig parse(Configuration config, Map<String, ?> rawProps) {
        final var nodeMode = NodeMode.parse(config.getString(Neo4jCudConverterConfig.NODE_MODE));
        final var nodeLabels = Strings.listOfTrimmed(config.getString(Neo4jCudConverterConfig.NODE_LABELS), Function.identity());
        final var nodeIdProperties = Strings.listOfTrimmed(config.getString(Neo4jCudConverterConfig.NODE_ID_PROPERTIES), Function.identity());
        final var propertiesInclude = toSet(Strings.listOfTrimmed(config.getString(Neo4jCudConverterConfig.NODE_PROPERTIES_INCLUDE), Function.identity()));
        final var propertiesExclude = toSet(Strings.listOfTrimmed(config.getString(Neo4jCudConverterConfig.NODE_PROPERTIES_EXCLUDE), Function.identity()));
        final var deleteDetach = config.getBoolean(Neo4jCudConverterConfig.NODE_DELETE_DETACH);
        final var outputMode = OutputMode.parse(config.getString(Neo4jCudConverterConfig.OUTPUT_MODE));
        final var tombstonesEnabled = config.getBoolean(Neo4jCudConverterConfig.TOMBSTONES_ENABLED);
        final var relationships = parseRelationships(nodeMode, rawProps);

        return new Neo4jCudConverterConfig(
                nodeMode, nodeLabels, nodeIdProperties,
                propertiesInclude, propertiesExclude, deleteDetach,
                outputMode, tombstonesEnabled, relationships);
    }

    private static Set<String> toSet(List<String> list) {
        if (list.isEmpty()) {
            return Collections.emptySet();
        }
        return Set.copyOf(list);
    }

    private static List<RelationshipMapping> parseRelationships(NodeMode nodeMode, Map<String, ?> rawProps) {
        final var grouped = groupByFkColumn(rawProps);
        final List<RelationshipMapping> mappings = new ArrayList<>();
        for (final var entry : grouped.entrySet()) {
            mappings.add(buildMapping(nodeMode, entry.getKey(), entry.getValue()));
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

    private static RelationshipMapping buildMapping(NodeMode nodeMode, String fkColumn, Map<String, String> subKeys) {
        final var type = requireNonBlank(subKeys.get("type"), RELATIONSHIP_PREFIX + fkColumn + ".type");
        final var targetLabel = requireNonBlank(subKeys.get("target.label"), RELATIONSHIP_PREFIX + fkColumn + ".target.label");

        final var direction = parseDirection(nodeMode, subKeys.get("direction"), RELATIONSHIP_PREFIX + fkColumn + ".direction");

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
                Strings.listOfTrimmed(subKeys.get("properties"), Function.identity()));
    }

    private static RelationshipMapping.Direction parseDirection(NodeMode nodeMode, String value, String key) {
        if (Strings.isNullOrBlank(value)) {
            // In relationship mode direction fixes the join endpoints (outgoing = source, incoming = target),
            // so it is required on both mappings and has no default.
            if (nodeMode == NodeMode.RELATIONSHIP) {
                throw new ConfigException(key, null, "Required when node.mode=relationship; must be 'outgoing' or 'incoming'");
            }
            return RelationshipMapping.Direction.OUTGOING;
        }
        return switch (value.toLowerCase()) {
            case "outgoing" -> RelationshipMapping.Direction.OUTGOING;
            case "incoming" -> RelationshipMapping.Direction.INCOMING;
            default -> throw new ConfigException(key, value, "Must be 'outgoing' or 'incoming'");
        };
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
        if (Strings.isNullOrBlank(value)) {
            throw new ConfigException(key, null, "Required");
        }
        return value;
    }
}
