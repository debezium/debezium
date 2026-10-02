/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.util.Strings;

/**
 * Top-level configuration for the {@code Neo4jDebeziumConverter} SMT (graph-to-relational).
 * <p>
 * The SMT is <em>zero-config by default</em>: Neo4j CDC events are self-describing, so every setting here is
 * an optional override. It holds the global naming/behavior settings shared by the whole SMT instance plus,
 * optionally, one {@link LabelMappingConfig} per configured node label and one {@link RelationshipMappingConfig}
 * per configured relationship type (optionally qualified by endpoint labels).
 */
public class Neo4jDebeziumConverterConfig {

    public static final Field TABLE_NAMING = Field.create("table.naming")
            .withDisplayName("Table naming strategy")
            .withType(ConfigDef.Type.STRING)
            .withDefault(NamingStrategy.AS_IS.getValue())
            .withEnum(NamingStrategy.class)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How a Neo4j label or relationship type becomes a target table name: 'as_is' keeps the "
                    + "Neo4j casing, 'snake_case' lowercases and snake-cases it.");

    public static final Field COLUMN_NAMING = Field.create("column.naming")
            .withDisplayName("Column naming strategy")
            .withType(ConfigDef.Type.STRING)
            .withDefault(NamingStrategy.AS_IS.getValue())
            .withEnum(NamingStrategy.class)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How a Neo4j property (and a composed foreign-key column name) becomes a relational column "
                    + "name: 'as_is' or 'snake_case'.");

    public static final Field RELATIONSHIP_FK_NAMING = Field.create("relationship.fk.naming")
            .withDisplayName("Foreign-key column naming pattern")
            .withType(ConfigDef.Type.STRING)
            .withDefault(FkNaming.LABEL_KEY.getValue())
            .withEnum(FkNaming.class)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The structure of a foreign-key / join-table endpoint column name before casing: "
                    + "'label_key' joins the endpoint label and key property with an underscore (e.g. Order_id). The "
                    + "composed name is then cased by column.naming.");

    public static final Field FIELD_MISSING_BEHAVIOR = Field.create("field.missing.behavior")
            .withDisplayName("Missing field behavior")
            .withType(ConfigDef.Type.STRING)
            .withDefault(FieldMissingBehavior.WARN.getValue())
            .withEnum(FieldMissingBehavior.class)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How to react when a Neo4j CDC event is missing something the mapping needs to build a "
                    + "Debezium envelope (an empty 'keys' map, no mapped owning label, an ambiguous multi-label node, "
                    + "a missing before/after image, or a primary-key value change): 'fail' throws so the record is "
                    + "routed by the connector's error handling; 'warn' logs and drops the record; 'ignore' drops it "
                    + "silently.");

    public static final Field TOMBSTONES_ENABLED = Field.create("tombstones.enabled")
            .withDisplayName("Tombstones enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Whether an incoming tombstone record (null value) is passed through unchanged (true) or "
                    + "dropped (false).");

    // Only the global keys are statically declared. Per-entity mapping keys (label.<Label>.* and
    // relationship.<TYPE>.*) are dynamic and parsed directly from the raw properties.
    public static final Field.Set ALL_FIELDS = Field.setOf(
            TABLE_NAMING, COLUMN_NAMING, RELATIONSHIP_FK_NAMING, FIELD_MISSING_BEHAVIOR, TOMBSTONES_ENABLED);

    private final NamingStrategy tableNaming;
    private final NamingStrategy columnNaming;
    private final FkNaming fkNaming;
    private final FieldMissingBehavior fieldMissingBehavior;
    private final boolean tombstonesEnabled;
    private final Map<String, LabelMappingConfig> labelMappings;
    private final Map<String, RelationshipMappingConfig> relationshipMappings;

    Neo4jDebeziumConverterConfig(NamingStrategy tableNaming, NamingStrategy columnNaming, FkNaming fkNaming,
                                 FieldMissingBehavior fieldMissingBehavior, boolean tombstonesEnabled,
                                 Map<String, LabelMappingConfig> labelMappings,
                                 Map<String, RelationshipMappingConfig> relationshipMappings) {
        this.tableNaming = tableNaming;
        this.columnNaming = columnNaming;
        this.fkNaming = fkNaming;
        this.fieldMissingBehavior = fieldMissingBehavior;
        this.tombstonesEnabled = tombstonesEnabled;
        this.labelMappings = labelMappings;
        this.relationshipMappings = relationshipMappings;
    }

    public static Neo4jDebeziumConverterConfig from(Configuration config, Map<String, ?> rawProps) {
        return Neo4jDebeziumConfigParser.parse(config, rawProps);
    }

    public NamingStrategy tableNaming() {
        return tableNaming;
    }

    public NamingStrategy columnNaming() {
        return columnNaming;
    }

    public FkNaming fkNaming() {
        return fkNaming;
    }

    public FieldMissingBehavior fieldMissingBehavior() {
        return fieldMissingBehavior;
    }

    public boolean tombstonesEnabled() {
        return tombstonesEnabled;
    }

    public Map<String, LabelMappingConfig> labelMappings() {
        return labelMappings;
    }

    public Map<String, RelationshipMappingConfig> relationshipMappings() {
        return relationshipMappings;
    }

    /**
     * Resolves the single owning label for a node from its labels: the one label that has an explicit
     * {@code label.<Label>.*} mapping. When no label is mapped, returns {@code null} (the caller applies the
     * convention only for single-label nodes). When more than one label is mapped, the owner is ambiguous and
     * an {@link AmbiguousLabelException} is thrown so the caller can treat it per {@code field.missing.behavior}.
     *
     * @throws AmbiguousLabelException when two or more of the node's labels each have a mapping
     */
    public LabelMappingConfig labelMappingFor(List<String> labels) {
        LabelMappingConfig matched = null;
        String matchedLabel = null;
        for (final var label : labels) {
            final var mapping = labelMappings.get(label);
            if (mapping != null) {
                if (matched != null) {
                    throw new AmbiguousLabelException(matchedLabel, label);
                }
                matched = mapping;
                matchedLabel = label;
            }
        }
        return matched;
    }

    /**
     * Resolves the mapping for a relationship, preferring an endpoint-qualified mapping
     * ({@code relationship.<TYPE>@<Start>-<End>.*}) over the unqualified {@code relationship.<TYPE>.*} fallback.
     * Returns {@code null} when neither is configured (the caller applies the convention).
     */
    public RelationshipMappingConfig relationshipMappingFor(String type, List<String> startLabels, List<String> endLabels) {
        for (final var startLabel : startLabels) {
            for (final var endLabel : endLabels) {
                final var qualified = relationshipMappings.get(qualifiedKey(type, startLabel, endLabel));
                if (qualified != null) {
                    return qualified;
                }
            }
        }
        return relationshipMappings.get(type);
    }

    static String qualifiedKey(String type, String startLabel, String endLabel) {
        return type + "@" + startLabel + "-" + endLabel;
    }

    /**
     * Thrown when a multi-label node has more than one label carrying a {@code label.<Label>.*} mapping, so the
     * owning label (and hence the target table and primary key) is genuinely ambiguous.
     */
    public static class AmbiguousLabelException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        AmbiguousLabelException(String first, String second) {
            super(String.format("Node has more than one mapped label ('%s' and '%s'); the owning label is ambiguous",
                    first, second));
        }
    }

    public enum NamingStrategy implements EnumeratedValue {
        AS_IS("as_is"),
        SNAKE_CASE("snake_case");

        private final String value;

        NamingStrategy(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public String apply(String name) {
            if (name == null) {
                return null;
            }
            return this == SNAKE_CASE ? Strings.toSnakeCase(name) : name;
        }

        public static NamingStrategy parse(String value) {
            return EnumeratedValue.parse(NamingStrategy.class, value, AS_IS.value);
        }
    }

    public enum FkNaming implements EnumeratedValue {
        LABEL_KEY("label_key");

        private final String value;

        FkNaming(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Composes the raw (uncased) foreign-key / join-endpoint column name from an endpoint label and its key
         * property, e.g. ({@code Order}, {@code id}) -&gt; {@code Order_id}. Casing is applied afterwards by
         * {@code column.naming}.
         */
        public String compose(String label, String keyProperty) {
            return label + "_" + keyProperty;
        }

        public static FkNaming parse(String value) {
            return EnumeratedValue.parse(FkNaming.class, value, LABEL_KEY.value);
        }
    }

    public enum RelationshipMode implements EnumeratedValue {
        JOIN_TABLE("join_table"),
        FOREIGN_KEY("foreign_key");

        private final String value;

        RelationshipMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static RelationshipMode parse(String value) {
            return EnumeratedValue.parse(RelationshipMode.class, value, JOIN_TABLE.value);
        }
    }

    public enum Owner implements EnumeratedValue {
        START("start"),
        END("end");

        private final String value;

        Owner(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static Owner parse(String value) {
            return EnumeratedValue.parse(Owner.class, value, START.value);
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
}
