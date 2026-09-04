/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;

/**
 * Top-level configuration for the Neo4j CUD converter.
 * Holds the global output settings shared by the whole SMT instance plus one
 * {@link TableMappingConfig} per configured source table. Mappings are keyed by the bare table name
 * and looked up per record via {@link #mappingFor(String)}.
 */
public class Neo4jCudConverterConfig {

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

    public static final Field FIELD_MISSING_BEHAVIOR = Field.create("field.missing.behavior")
            .withDisplayName("Missing field behavior")
            .withType(ConfigDef.Type.STRING)
            .withDefault(FieldMissingBehavior.WARN.getValue())
            .withEnum(FieldMissingBehavior.class)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How to react when a change event is missing a column that the mapping needs to build a "
                    + "CUD event (an id property, a foreign key, or the before/after image): 'fail' throws so the "
                    + "record is routed by the connector's error handling; 'warn' logs and skips the affected event "
                    + "(or the whole record when nothing can be built); 'ignore' skips silently. A node whose entire "
                    + "id key resolves to empty is never emitted regardless of this setting, because an unkeyed MERGE "
                    + "would collapse every row into a single node.");

    // Only the global keys are statically declared. Per-table mapping keys (table.<name>.*) are
    // dynamic and parsed directly from the raw properties, like relationship.<fk>.* subkeys.
    public static final Field.Set ALL_FIELDS = Field.setOf(OUTPUT_MODE, TOMBSTONES_ENABLED, FIELD_MISSING_BEHAVIOR);

    private final OutputMode outputMode;
    private final boolean tombstonesEnabled;
    private final FieldMissingBehavior fieldMissingBehavior;
    private final Map<String, TableMappingConfig> tableMappings;

    Neo4jCudConverterConfig(OutputMode outputMode, boolean tombstonesEnabled, FieldMissingBehavior fieldMissingBehavior,
                            Map<String, TableMappingConfig> tableMappings) {
        this.outputMode = outputMode;
        this.tombstonesEnabled = tombstonesEnabled;
        this.fieldMissingBehavior = fieldMissingBehavior;
        this.tableMappings = tableMappings;

        validate();
    }

    public static Neo4jCudConverterConfig from(Configuration config, Map<String, ?> rawProps) {
        return Neo4jCudConfigParser.parse(config, rawProps);
    }

    private void validate() {
        if (tableMappings.isEmpty()) {
            throw new ConfigException("table.<table_name>.*", null,
                    "At least one per-table mapping is required, for example table.<table_name>.node.id.properties");
        }
    }

    public OutputMode outputMode() {
        return outputMode;
    }

    public boolean tombstonesEnabled() {
        return tombstonesEnabled;
    }

    public FieldMissingBehavior fieldMissingBehavior() {
        return fieldMissingBehavior;
    }

    public Map<String, TableMappingConfig> tableMappings() {
        return tableMappings;
    }

    /**
     * Returns the mapping configured for the given source table, or {@code null} when no mapping is
     * configured (the record is passed through unchanged) or when {@code table} is {@code null}.
     */
    public TableMappingConfig mappingFor(String table) {
        return table == null ? null : tableMappings.get(table);
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

    public enum FieldMissingBehavior implements EnumeratedValue {
        FAIL("fail"),
        WARN("warn"),
        IGNORE("ignore");

        private final String value;

        FieldMissingBehavior(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static FieldMissingBehavior parse(String value) {
            return EnumeratedValue.parse(FieldMissingBehavior.class, value, WARN.value);
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
