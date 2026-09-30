/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.Configuration;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.FieldMissingBehavior;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.FkNaming;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.NamingStrategy;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.Owner;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.RelationshipMode;
import io.debezium.util.Strings;

/**
 * Parses the dynamic per-entity configuration keys for {@code Neo4jDebeziumConverter}.
 * <p>
 * It is a stateless utility that strips the {@code label.<Label>.} and
 * {@code relationship.<TYPE>[@<Start>-<End>].} prefixes by hand (no regex), groups the remaining sub-keys by
 * entity, and fails fast on any sub-key that is not in the recognized allow-list.
 */
class Neo4jDebeziumConfigParser {

    private static final String LABEL_PREFIX = "label.";
    private static final String RELATIONSHIP_PREFIX = "relationship.";

    private static final Set<String> KNOWN_LABEL_KEYS = Set.of(
            "table", "key.properties", "properties.include", "properties.exclude");
    private static final Set<String> KNOWN_RELATIONSHIP_KEYS = Set.of(
            "mode", "table", "start.column", "end.column", "properties", "owner", "fk.column");
    // relationship.fk.naming is a global key that happens to share the relationship. prefix; it must not be
    // treated as a per-relationship mapping for a type literally named "fk".
    private static final String RELATIONSHIP_FK_NAMING_SUFFIX = "fk.naming";

    static Neo4jDebeziumConverterConfig parse(Configuration config, Map<String, ?> rawProps) {
        final var tableNaming = NamingStrategy.parse(
                Neo4jDebeziumConverterConfig.TABLE_NAMING.name(),
                config.getString(Neo4jDebeziumConverterConfig.TABLE_NAMING));
        final var columnNaming = NamingStrategy.parse(
                Neo4jDebeziumConverterConfig.COLUMN_NAMING.name(),
                config.getString(Neo4jDebeziumConverterConfig.COLUMN_NAMING));
        final var fkNaming = FkNaming.parse(
                Neo4jDebeziumConverterConfig.RELATIONSHIP_FK_NAMING.name(),
                config.getString(Neo4jDebeziumConverterConfig.RELATIONSHIP_FK_NAMING));
        final var fieldMissingBehavior = FieldMissingBehavior.parse(
                Neo4jDebeziumConverterConfig.FIELD_MISSING_BEHAVIOR.name(),
                config.getString(Neo4jDebeziumConverterConfig.FIELD_MISSING_BEHAVIOR));
        final var tombstonesEnabled = config.getBoolean(Neo4jDebeziumConverterConfig.TOMBSTONES_ENABLED);

        final var labelMappings = buildLabelMappings(groupByEntity(rawProps, LABEL_PREFIX));
        final var relationshipMappings = buildRelationshipMappings(groupByEntity(rawProps, RELATIONSHIP_PREFIX));

        return new Neo4jDebeziumConverterConfig(
                tableNaming, columnNaming, fkNaming, fieldMissingBehavior, tombstonesEnabled,
                Collections.unmodifiableMap(labelMappings),
                Collections.unmodifiableMap(relationshipMappings));
    }

    /**
     * Groups the dynamic {@code <prefix><entity>.<subKey>} properties by entity, stripping the prefix from each
     * key. The entity is everything between the prefix and the first following dot (so it may carry an
     * {@code @Start-End} qualifier, which uses {@code -} not {@code .}).
     */
    private static Map<String, Map<String, String>> groupByEntity(Map<String, ?> rawProps, String prefix) {
        final Map<String, Map<String, String>> grouped = new LinkedHashMap<>();
        for (final var entry : rawProps.entrySet()) {
            final var key = entry.getKey();
            if (!key.startsWith(prefix)) {
                continue;
            }
            final var withoutPrefix = key.substring(prefix.length());
            // relationship.fk.naming is a global key, not a per-type mapping.
            if (prefix.equals(RELATIONSHIP_PREFIX) && withoutPrefix.equals(RELATIONSHIP_FK_NAMING_SUFFIX)) {
                continue;
            }
            final var dotIndex = withoutPrefix.indexOf('.');
            if (dotIndex < 0) {
                continue;
            }
            final var entity = withoutPrefix.substring(0, dotIndex);
            final var subKey = withoutPrefix.substring(dotIndex + 1);
            grouped.computeIfAbsent(entity, k -> new LinkedHashMap<>())
                    .put(subKey, String.valueOf(entry.getValue()));
        }
        return grouped;
    }

    private static Map<String, LabelMappingConfig> buildLabelMappings(Map<String, Map<String, String>> byLabel) {
        final Map<String, LabelMappingConfig> mappings = new LinkedHashMap<>();
        for (final var entry : byLabel.entrySet()) {
            final var label = entry.getKey();
            final var subKeys = entry.getValue();
            validateKnownKeys(LABEL_PREFIX, label, subKeys, KNOWN_LABEL_KEYS);

            mappings.put(label, new LabelMappingConfig(
                    label,
                    subKeys.get("table"),
                    Strings.listOfTrimmed(subKeys.get("key.properties"), Function.identity()),
                    toSet(Strings.listOfTrimmed(subKeys.get("properties.include"), Function.identity())),
                    toSet(Strings.listOfTrimmed(subKeys.get("properties.exclude"), Function.identity()))));
        }
        return mappings;
    }

    private static Map<String, RelationshipMappingConfig> buildRelationshipMappings(Map<String, Map<String, String>> byType) {
        final Map<String, RelationshipMappingConfig> mappings = new LinkedHashMap<>();
        for (final var entry : byType.entrySet()) {
            final var entity = entry.getKey();
            final var subKeys = entry.getValue();
            validateKnownKeys(RELATIONSHIP_PREFIX, entity, subKeys, KNOWN_RELATIONSHIP_KEYS);

            final var atIndex = entity.indexOf('@');
            final var type = atIndex < 0 ? entity : entity.substring(0, atIndex);
            final var qualifier = atIndex < 0 ? null : entity.substring(atIndex + 1);

            final var mode = RelationshipMode.parse(RELATIONSHIP_PREFIX + entity + ".mode", subKeys.get("mode"));
            final var owner = Owner.parse(RELATIONSHIP_PREFIX + entity + ".owner", subKeys.get("owner"));
            validateModeKeys(entity, subKeys, mode);

            mappings.put(entity, new RelationshipMappingConfig(
                    type,
                    qualifier,
                    mode,
                    subKeys.get("table"),
                    subKeys.get("start.column"),
                    subKeys.get("end.column"),
                    Strings.listOfTrimmed(subKeys.get("properties"), Function.identity()),
                    owner,
                    subKeys.get("fk.column")));
        }
        return mappings;
    }

    private static void validateModeKeys(String entity, Map<String, String> subKeys, RelationshipMode mode) {
        if (mode == RelationshipMode.JOIN_TABLE) {
            if (subKeys.containsKey("fk.column")) {
                throw new ConfigException(RELATIONSHIP_PREFIX + entity + ".fk.column", subKeys.get("fk.column"),
                        "Only valid when mode=foreign_key");
            }
            if (subKeys.containsKey("owner")) {
                throw new ConfigException(RELATIONSHIP_PREFIX + entity + ".owner", subKeys.get("owner"),
                        "Only valid when mode=foreign_key");
            }
        }
        else {
            if (subKeys.containsKey("start.column")) {
                throw new ConfigException(RELATIONSHIP_PREFIX + entity + ".start.column", subKeys.get("start.column"),
                        "Only valid when mode=join_table");
            }
            if (subKeys.containsKey("end.column")) {
                throw new ConfigException(RELATIONSHIP_PREFIX + entity + ".end.column", subKeys.get("end.column"),
                        "Only valid when mode=join_table");
            }
        }
    }

    /**
     * Rejects any sub-key that is not a recognized structural option, so typos fail fast at configure() time
     * instead of being silently dropped.
     */
    private static void validateKnownKeys(String prefix, String entity, Map<String, String> subKeys, Set<String> known) {
        for (final var subKey : subKeys.keySet()) {
            if (!known.contains(subKey)) {
                throw new ConfigException(prefix + entity + "." + subKey, null,
                        "Unknown configuration property for '" + entity + "'");
            }
        }
    }

    private static Set<String> toSet(List<String> list) {
        return list.isEmpty() ? Collections.emptySet() : Set.copyOf(list);
    }
}
