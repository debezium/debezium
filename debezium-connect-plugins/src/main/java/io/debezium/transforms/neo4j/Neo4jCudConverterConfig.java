/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;

public class Neo4jCudConverterConfig {

    public static final Field NODE_MODE = Field.create("node.mode")
            .withDisplayName("Node mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault(NodeMode.NODE.getValue())
            .withEnum(NodeMode.class)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Controls the entity type: 'node' treats the table row as a graph node; "
                    + "'relationship' treats it as a graph relationship (for join tables).");

    public static final Field NODE_LABELS = Field.create("node.labels")
            .withDisplayName("Node labels")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated Neo4j labels. Defaults to PascalCase singular of the source table name.");

    public static final Field NODE_ID_PROPERTIES = Field.create("node.id.properties")
            .withDisplayName("Node ID properties")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated column names used as CUD key properties (ids).");

    public static final Field NODE_PROPERTIES_INCLUDE = Field.create("node.properties.include")
            .withDisplayName("Included properties")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated columns to include as node properties. Mutually exclusive with node.properties.exclude.");

    public static final Field NODE_PROPERTIES_EXCLUDE = Field.create("node.properties.exclude")
            .withDisplayName("Excluded properties")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated columns to exclude from node properties. FK columns mapped to relationships are excluded automatically.");

    public static final Field NODE_DELETE_DETACH = Field.create("node.delete.detach")
            .withDisplayName("Detach on delete")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether to set detach=true on delete operations.");

    public static final Field OUTPUT_MODE = Field.create("output.mode")
            .withDisplayName("Output mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault(OutputMode.ARRAY.getValue())
            .withEnum(OutputMode.class)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("'array' packs all CUD events into a JSON array; 'single' produces one CUD event per record.");

    public static final Field TOMBSTONES_ENABLED = Field.create("tombstones.enabled")
            .withDisplayName("Tombstones enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Whether to pass through tombstone records.");

    public static final Field.Set ALL_FIELDS = Field.setOf(
            NODE_MODE, NODE_LABELS, NODE_ID_PROPERTIES,
            NODE_PROPERTIES_INCLUDE, NODE_PROPERTIES_EXCLUDE, NODE_DELETE_DETACH,
            OUTPUT_MODE, TOMBSTONES_ENABLED);

    private final NodeMode nodeMode;
    private final List<String> nodeLabels;
    private final List<String> nodeIdProperties;
    private final Set<String> propertiesInclude;
    private final Set<String> propertiesExclude;
    private final boolean deleteDetach;
    private final OutputMode outputMode;
    private final boolean tombstonesEnabled;
    private final List<RelationshipMapping> relationships;
    private final Set<String> fkColumns;

    Neo4jCudConverterConfig(NodeMode nodeMode, List<String> nodeLabels, List<String> nodeIdProperties,
                            Set<String> propertiesInclude, Set<String> propertiesExclude, boolean deleteDetach,
                            OutputMode outputMode, boolean tombstonesEnabled, List<RelationshipMapping> relationships) {
        this.nodeMode = nodeMode;
        this.nodeLabels = nodeLabels;
        this.nodeIdProperties = nodeIdProperties;
        this.propertiesInclude = propertiesInclude;
        this.propertiesExclude = propertiesExclude;
        this.deleteDetach = deleteDetach;
        this.outputMode = outputMode;
        this.tombstonesEnabled = tombstonesEnabled;
        this.relationships = relationships;
        this.fkColumns = relationships.stream()
                .map(RelationshipMapping::fkColumn)
                .collect(Collectors.toSet());

        validate();
    }

    public static Neo4jCudConverterConfig from(Configuration config, Map<String, ?> rawProps) {
        return Neo4jCudConfigParser.parse(config, rawProps);
    }

    private void validate() {
        if (nodeMode == NodeMode.NODE && nodeIdProperties.isEmpty()) {
            throw new ConfigException("node.id.properties", null, "Required when node.mode=node");
        }
        if (nodeMode == NodeMode.RELATIONSHIP) {
            if (relationships.size() != 2) {
                throw new ConfigException("relationship.*", relationships.size(),
                        "node.mode=relationship requires exactly two relationship mappings (a source and a target endpoint)");
            }
            final var outgoing = relationships.stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.OUTGOING).count();
            final var incoming = relationships.stream()
                    .filter(r -> r.direction() == RelationshipMapping.Direction.INCOMING).count();
            if (outgoing != 1 || incoming != 1) {
                throw new ConfigException("relationship.<fk_column>.direction", null,
                        "node.mode=relationship requires exactly one outgoing (source) and one incoming (target) relationship endpoint");
            }
        }
        if (!propertiesInclude.isEmpty() && !propertiesExclude.isEmpty()) {
            throw new ConfigException("node.properties.include and node.properties.exclude are mutually exclusive");
        }
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

    public OutputMode outputMode() {
        return outputMode;
    }

    public boolean tombstonesEnabled() {
        return tombstonesEnabled;
    }

    public List<RelationshipMapping> relationships() {
        return relationships;
    }

    public Set<String> fkColumns() {
        return fkColumns;
    }

    public enum NodeMode implements EnumeratedValue {
        NODE("node"),
        RELATIONSHIP("relationship");

        private final String value;

        NodeMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static NodeMode parse(String value) {
            return EnumeratedValue.parse(NodeMode.class, value, NODE.value);
        }
    }

    public enum OutputMode implements EnumeratedValue {
        ARRAY("array"),
        SINGLE("single");

        private final String value;

        OutputMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static OutputMode parse(String value) {
            return EnumeratedValue.parse(OutputMode.class, value, ARRAY.value);
        }
    }
}
