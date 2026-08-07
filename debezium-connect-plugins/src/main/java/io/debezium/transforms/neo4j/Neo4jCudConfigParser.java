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

    private static final String TABLE_PREFIX = "table.";
    private static final String RELATIONSHIP_PREFIX = "relationship.";

    // Recognized structural sub-keys within a table.<name>. namespace. Anything else is a typo or an
    // unsupported option and is rejected at configure() time rather than silently ignored.
    private static final Set<String> KNOWN_NODE_KEYS = Set.of(
            "node.mode", "node.labels", "node.id.properties",
            "node.properties.include", "node.properties.exclude", "node.delete.detach");
    private static final Set<String> KNOWN_RELATIONSHIP_KEYS = Set.of(
            "type", "direction", "target.label", "target.id", "target.node.op", "properties");

    static Neo4jCudConverterConfig parse(Configuration config, Map<String, ?> rawProps) {
        final var outputMode = OutputMode.parse(config.getString(Neo4jCudConverterConfig.OUTPUT_MODE));
        final var tombstonesEnabled = config.getBoolean(Neo4jCudConverterConfig.TOMBSTONES_ENABLED);

        final var byTable = groupByTable(rawProps);
        final Map<String, TableMappingConfig> tableMappings = new LinkedHashMap<>();
        for (final var entry : byTable.entrySet()) {
            tableMappings.put(entry.getKey(), buildTableMapping(entry.getKey(), entry.getValue()));
        }

        return new Neo4jCudConverterConfig(
                outputMode, tombstonesEnabled, Collections.unmodifiableMap(tableMappings));
    }

    /**
     * Groups the dynamic {@code table.<tableName>.<subKey>} properties by table name, stripping the
     * {@code table.<tableName>.} prefix from each key. Mirrors {@link #groupByFkColumn} one level up.
     */
    private static Map<String, Map<String, String>> groupByTable(Map<String, ?> rawProps) {
        final Map<String, Map<String, String>> grouped = new LinkedHashMap<>();
        for (final var entry : rawProps.entrySet()) {
            final var key = entry.getKey();
            if (!key.startsWith(TABLE_PREFIX)) {
                continue;
            }
            final var withoutPrefix = key.substring(TABLE_PREFIX.length());
            final var dotIndex = withoutPrefix.indexOf('.');
            if (dotIndex < 0) {
                continue;
            }
            final var tableName = withoutPrefix.substring(0, dotIndex);
            final var subKey = withoutPrefix.substring(dotIndex + 1);
            grouped.computeIfAbsent(tableName, k -> new LinkedHashMap<>())
                    .put(subKey, String.valueOf(entry.getValue()));
        }
        return grouped;
    }

    private static TableMappingConfig buildTableMapping(String tableName, Map<String, String> subKeys) {
        validateKnownKeys(tableName, subKeys);

        final var nodeMode = NodeMode.parse(subKeys.get("node.mode"));
        final var nodeLabels = Strings.listOfTrimmed(subKeys.get("node.labels"), Function.identity());
        final var nodeIdProperties = Strings.listOfTrimmed(subKeys.get("node.id.properties"), Function.identity());
        final var propertiesInclude = toSet(Strings.listOfTrimmed(subKeys.get("node.properties.include"), Function.identity()));
        final var propertiesExclude = toSet(Strings.listOfTrimmed(subKeys.get("node.properties.exclude"), Function.identity()));
        final var deleteDetach = subKeys.containsKey("node.delete.detach")
                ? Boolean.parseBoolean(subKeys.get("node.delete.detach"))
                : true;
        final var relationships = parseRelationships(nodeMode, subKeys);

        return new TableMappingConfig(
                tableName, nodeMode, nodeLabels, nodeIdProperties,
                propertiesInclude, propertiesExclude, deleteDetach, relationships);
    }

    /**
     * Rejects any sub-key under a {@code table.<name>.} namespace that is not a recognized structural
     * option, so typos (for example {@code node.lable}) fail fast instead of being silently dropped.
     * Relationship keys are validated on their sub-key ({@code type}, {@code target.label}, ...), with
     * any non-blank foreign-key column name accepted between {@code relationship.} and that sub-key.
     */
    private static void validateKnownKeys(String tableName, Map<String, String> subKeys) {
        for (final var subKey : subKeys.keySet()) {
            if (KNOWN_NODE_KEYS.contains(subKey)) {
                continue;
            }
            if (subKey.startsWith(RELATIONSHIP_PREFIX)) {
                final var withoutPrefix = subKey.substring(RELATIONSHIP_PREFIX.length());
                final var dotIndex = withoutPrefix.indexOf('.');
                if (dotIndex > 0 && KNOWN_RELATIONSHIP_KEYS.contains(withoutPrefix.substring(dotIndex + 1))) {
                    continue;
                }
            }
            throw new ConfigException(TABLE_PREFIX + tableName + "." + subKey, null,
                    "Unknown configuration property for table '" + tableName + "'");
        }
    }

    private static Set<String> toSet(List<String> list) {
        if (list.isEmpty()) {
            return Collections.emptySet();
        }
        return Set.copyOf(list);
    }

    private static List<RelationshipMapping> parseRelationships(NodeMode nodeMode, Map<String, String> subKeys) {
        final var grouped = groupByFkColumn(subKeys);
        final List<RelationshipMapping> mappings = new ArrayList<>();
        for (final var entry : grouped.entrySet()) {
            mappings.add(buildMapping(nodeMode, entry.getKey(), entry.getValue()));
        }
        return Collections.unmodifiableList(mappings);
    }

    private static Map<String, Map<String, String>> groupByFkColumn(Map<String, String> subKeys) {
        final Map<String, Map<String, String>> grouped = new LinkedHashMap<>();
        for (final var entry : subKeys.entrySet()) {
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
                    .put(subKey, entry.getValue());
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
