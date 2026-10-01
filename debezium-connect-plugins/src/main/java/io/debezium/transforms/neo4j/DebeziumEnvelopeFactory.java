/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.data.Envelope;
import io.debezium.transforms.neo4j.Neo4jCdcEvent.Endpoint;
import io.debezium.transforms.neo4j.Neo4jCdcEvent.Operation;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.AmbiguousLabelException;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.FieldMissingBehavior;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.NamingStrategy;
import io.debezium.transforms.neo4j.Neo4jTypeMapper.Converted;

/**
 * Builds a Debezium envelope record (value {@link Struct} + primary-key {@link Struct} + target table name) from
 * a single Neo4j CDC change event, applying the convention-based default mapping and any configured overrides.
 * Mirrors {@code CudEventFactory} in the opposite direction.
 * <p>
 * {@link #build(Neo4jCdcEvent)} returns a {@link BuildResult}: either the emitted record, or a skip reason when the
 * event cannot be converted and {@code field.missing.behavior} says to skip rather than fail. The caller drops
 * records that carry no emitted record.
 * <p>
 * Property and key values arrive as {@code Neo4jPropertyType} union structs (EXTENDED payload), so the {@code before}/
 * {@code after} images and the endpoint keys are handled as maps from column name to that union struct, each converted
 * to a relational column by {@link Neo4jTypeMapper}.
 */
public class DebeziumEnvelopeFactory {

    private static final String CONNECTOR_NAME = "neo4j";

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.neo4j.Source")
            .field("connector", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();

    private final Neo4jDebeziumConverterConfig config;
    private final FieldMissingBehavior fieldMissingBehavior;
    private final NamingStrategy tableNaming;
    private final NamingStrategy columnNaming;

    public DebeziumEnvelopeFactory(Neo4jDebeziumConverterConfig config) {
        this.config = config;
        this.fieldMissingBehavior = config.fieldMissingBehavior();
        this.tableNaming = config.tableNaming();
        this.columnNaming = config.columnNaming();
    }

    public BuildResult build(Neo4jCdcEvent event) {
        try {
            return BuildResult.of(buildRecord(event));
        }
        catch (SkipRecordException e) {
            return BuildResult.skipped(e.getMessage());
        }
    }

    private EmittedRecord buildRecord(Neo4jCdcEvent event) {
        final var op = event.operation();
        if (op == null) {
            throw onMissingField("Neo4j CDC event has an unrecognized or absent operation", "skipping record");
        }
        return event.isNode() ? buildNode(event, op) : buildRelationship(event, op);
    }

    private EmittedRecord buildNode(Neo4jCdcEvent event, Operation op) {
        final var labels = event.labels();
        final LabelMappingConfig mapping;
        try {
            mapping = config.labelMappingFor(labels);
        }
        catch (AmbiguousLabelException e) {
            throw onMissingField(e.getMessage(), "skipping record");
        }

        final String owningLabel = resolveOwningLabel(labels, mapping);
        final var keyNames = resolveKeyNames(event, owningLabel, mapping);

        if (op.isUpdate() && keyValueChanged(event, keyNames)) {
            throw onMissingField(String.format(
                    "Update for label '%s' changes a primary-key value, which a stateless one-record SMT cannot "
                            + "represent as the delete+create pair the JDBC sink expects",
                    owningLabel),
                    "skipping record");
        }

        final var image = op.isDelete() ? event.beforeProperties() : event.afterProperties();
        if (image.isEmpty()) {
            throw onMissingField(String.format("Node event for label '%s' (op=%s) has no %s image",
                    owningLabel, op.code(), op.isDelete() ? "before" : "after"),
                    "skipping record");
        }

        final var table = tableName(owningLabel, mapping == null ? null : mapping.table());

        final var rowColumns = nodeRowColumns(image, keyNames, mapping);
        final var keyColumns = keyColumns(image, keyNames);

        return emit(table, op, keyColumns, rowColumns, event);
    }

    private String resolveOwningLabel(List<String> labels, LabelMappingConfig mapping) {
        if (mapping != null) {
            return mapping.label();
        }
        if (labels.isEmpty()) {
            throw onMissingField("Node event has no labels; cannot resolve a target table", "skipping record");
        }
        if (labels.size() > 1) {
            throw onMissingField(String.format(
                    "Multi-label node %s has no label.<Label> mapping, so the owning label is unknown", labels),
                    "skipping record");
        }
        return labels.get(0);
    }

    /** Resolves the primary-key property names: the override, else the label's {@code keys} constraint entry. */
    private List<String> resolveKeyNames(Neo4jCdcEvent event, String owningLabel, LabelMappingConfig mapping) {
        if (mapping != null && !mapping.keyProperties().isEmpty()) {
            return mapping.keyProperties();
        }
        final var keys = event.keysFor(owningLabel);
        if (keys == null || keys.isEmpty()) {
            throw onMissingField(String.format(
                    "Label '%s' has no 'keys' entry (no Neo4j key/uniqueness constraint), so no portable primary key "
                            + "can be derived",
                    owningLabel),
                    "skipping record");
        }
        return List.copyOf(keys.keySet());
    }

    private boolean keyValueChanged(Neo4jCdcEvent event, List<String> keyNames) {
        final var before = event.beforeProperties();
        final var after = event.afterProperties();
        if (before.isEmpty() || after.isEmpty()) {
            return false;
        }
        for (final var name : keyNames) {
            final var beforeValue = before.get(name);
            final var afterValue = after.get(name);
            if (beforeValue != null && afterValue != null
                    && !Objects.equals(Neo4jTypeMapper.convert(beforeValue).value(),
                            Neo4jTypeMapper.convert(afterValue).value())) {
                return true;
            }
        }
        return false;
    }

    private Map<String, Converted> nodeRowColumns(Map<String, Struct> image, List<String> keyNames,
                                                  LabelMappingConfig mapping) {
        final var include = mapping == null ? Collections.<String> emptySet() : mapping.propertiesInclude();
        final var exclude = mapping == null ? Collections.<String> emptySet() : mapping.propertiesExclude();
        final var useInclude = !include.isEmpty();

        // Sort by property name so the emitted row schema is deterministic regardless of the (unspecified)
        // property order in the incoming CDC image.
        final Map<String, Converted> columns = new LinkedHashMap<>();
        for (final var name : image.keySet().stream().sorted().toList()) {
            final var isKey = keyNames.contains(name);
            if (!isKey && useInclude && !include.contains(name)) {
                continue;
            }
            if (!isKey && exclude.contains(name)) {
                continue;
            }
            columns.put(columnNaming.apply(name), Neo4jTypeMapper.convert(image.get(name)));
        }
        return columns;
    }

    private Map<String, Converted> keyColumns(Map<String, Struct> image, List<String> keyNames) {
        final Map<String, Converted> columns = new LinkedHashMap<>();
        for (final var name : keyNames) {
            final var propertyType = image.get(name);
            if (propertyType == null) {
                throw onMissingField(String.format("Primary-key property '%s' is absent from the change event image", name),
                        "skipping record");
            }
            columns.put(columnNaming.apply(name), requiredKey(Neo4jTypeMapper.convert(propertyType)));
        }
        return columns;
    }

    /** A primary-key column must not be nullable in the relational target, so force its schema to required. */
    private static Converted requiredKey(Converted converted) {
        return new Converted(Neo4jTypeMapper.required(converted.schema()), converted.value());
    }

    private EmittedRecord buildRelationship(Neo4jCdcEvent event, Operation op) {
        final var type = event.type();
        final var start = event.start();
        final var end = event.end();
        if (type == null || start == null || end == null) {
            throw onMissingField("Relationship event is missing its type or an endpoint", "skipping record");
        }

        final var mapping = config.relationshipMappingFor(type, start.labels(), end.labels());
        if (mapping != null && mapping.isForeignKey()) {
            return buildForeignKeyRelationship(event, op, mapping, start, end);
        }
        return buildJoinTableRelationship(event, op, type, mapping, start, end);
    }

    private EmittedRecord buildJoinTableRelationship(Neo4jCdcEvent event, Operation op, String type,
                                                     RelationshipMappingConfig mapping, Endpoint start, Endpoint end) {
        final var table = tableName(type, mapping == null ? null : mapping.table());

        final var startColumns = endpointColumns(start, mapping == null ? null : mapping.startColumn());
        final var endColumns = endpointColumns(end, mapping == null ? null : mapping.endColumn());

        final Map<String, Converted> keyColumns = new LinkedHashMap<>();
        keyColumns.putAll(startColumns);
        keyColumns.putAll(endColumns);

        final Map<String, Converted> rowColumns = new LinkedHashMap<>(keyColumns);
        if (!op.isDelete()) {
            rowColumns.putAll(relationshipProperties(event, mapping));
        }

        return emit(table, op, keyColumns, rowColumns, event);
    }

    private EmittedRecord buildForeignKeyRelationship(Neo4jCdcEvent event, Operation op,
                                                      RelationshipMappingConfig mapping, Endpoint start, Endpoint end) {
        final var ownerIsStart = mapping.owner() == Neo4jDebeziumConverterConfig.Owner.START;
        final var owner = ownerIsStart ? start : end;
        final var other = ownerIsStart ? end : start;

        final var ownerLabel = labelWithKey(owner);
        final var otherLabel = labelWithKey(other);
        if (ownerLabel == null || otherLabel == null) {
            throw onMissingField("Foreign-key relationship endpoint has no key columns", "skipping record");
        }

        final var ownerKey = owner.keysFor(ownerLabel);
        final var otherKey = other.keysFor(otherLabel);
        if (ownerKey == null || ownerKey.isEmpty() || otherKey == null || otherKey.isEmpty()) {
            throw onMissingField(String.format("Foreign-key relationship is missing endpoint keys for '%s' or '%s'",
                    ownerLabel, otherLabel), "skipping record");
        }

        final var table = tableName(ownerLabel, mapping.table());

        // The owner row key: its own PK columns (named like the node's columns).
        final Map<String, Converted> keyColumns = new LinkedHashMap<>();
        for (final var entry : ownerKey.entrySet()) {
            keyColumns.put(columnNaming.apply(entry.getKey()), requiredKey(Neo4jTypeMapper.convert(entry.getValue())));
        }

        // The foreign-key column(s): the other endpoint's key, set to its value on create/update, null on delete.
        final Map<String, Converted> rowColumns = new LinkedHashMap<>(keyColumns);
        final var single = otherKey.size() == 1;
        for (final var entry : otherKey.entrySet()) {
            final var name = single && mapping.fkColumn() != null
                    ? mapping.fkColumn()
                    : columnNaming.apply(config.fkNaming().compose(otherLabel, entry.getKey()));
            final var converted = Neo4jTypeMapper.convert(entry.getValue());
            rowColumns.put(name, new Converted(Neo4jTypeMapper.optional(converted.schema()),
                    op.isDelete() ? null : converted.value()));
        }

        // A relationship FK change is always a partial update of the owner row (DDD-74).
        return emit(table, Operation.UPDATE, keyColumns, rowColumns, event);
    }

    /**
     * Produces the join-table columns for one endpoint from its key map: each key property becomes a column
     * named by the {@code start.column}/{@code end.column} override (single-key only) or by
     * {@code relationship.fk.naming} + {@code column.naming}.
     */
    private Map<String, Converted> endpointColumns(Endpoint endpoint, String columnOverride) {
        final var label = labelWithKey(endpoint);
        if (label == null) {
            throw onMissingField("Relationship endpoint has no labels", "skipping record");
        }
        final var keys = endpoint.keysFor(label);
        if (keys == null || keys.isEmpty()) {
            throw onMissingField(String.format("Relationship endpoint label '%s' has no key columns", label),
                    "skipping record");
        }

        final Map<String, Converted> columns = new LinkedHashMap<>();
        final var single = keys.size() == 1;
        for (final var entry : keys.entrySet()) {
            final var name = single && columnOverride != null
                    ? columnOverride
                    : columnNaming.apply(config.fkNaming().compose(label, entry.getKey()));
            // These endpoint columns form the join table's composite primary key, so they must be required.
            columns.put(name, requiredKey(Neo4jTypeMapper.convert(entry.getValue())));
        }
        return columns;
    }

    private Map<String, Converted> relationshipProperties(Neo4jCdcEvent event, RelationshipMappingConfig mapping) {
        final var props = event.afterProperties();
        if (props.isEmpty()) {
            return Collections.emptyMap();
        }
        final var include = mapping == null ? List.<String> of() : mapping.properties();
        final var useInclude = !include.isEmpty();

        // Sort by property name so the emitted column order is deterministic regardless of CDC property order.
        final Map<String, Converted> columns = new LinkedHashMap<>();
        for (final var name : props.keySet().stream().sorted().toList()) {
            if (useInclude && !include.contains(name)) {
                continue;
            }
            columns.put(columnNaming.apply(name), Neo4jTypeMapper.convert(props.get(name)));
        }
        return columns;
    }

    private static String labelWithKey(Endpoint endpoint) {
        final var labels = endpoint.labels();
        for (final var label : labels) {
            final var keys = endpoint.keysFor(label);
            if (keys != null && !keys.isEmpty()) {
                return label;
            }
        }
        return labels.isEmpty() ? null : labels.get(0);
    }

    private EmittedRecord emit(String table, Operation op, Map<String, Converted> keyColumns,
                               Map<String, Converted> rowColumns, Neo4jCdcEvent event) {
        final var keyStruct = structFromColumns(table + ".Key", keyColumns);
        final var rowStruct = structFromColumns(table + ".Value", rowColumns);
        final var source = buildSource(table, event);

        final var envelope = Envelope.defineSchema()
                .withName(table + ".Envelope")
                .withRecord(rowStruct.schema())
                .withSource(SOURCE_SCHEMA)
                .build();

        final var now = Instant.now();
        final Struct payload = switch (op) {
            case CREATE -> envelope.create(rowStruct, source, now);
            // 'before' is null: the JDBC sink upserts from 'after' by primary key and never reads 'before',
            // and a Neo4j CDC 'after' image already carries the full post-update row (not just changed props).
            case UPDATE -> envelope.update(null, rowStruct, source, now);
            case DELETE -> envelope.delete(rowStruct, source, now);
        };

        return new EmittedRecord(table, keyStruct.schema(), keyStruct, envelope.schema(), payload);
    }

    private static Struct structFromColumns(String schemaName, Map<String, Converted> columns) {
        final var builder = SchemaBuilder.struct().name(schemaName);
        for (final var entry : columns.entrySet()) {
            builder.field(entry.getKey(), entry.getValue().schema());
        }
        final var schema = builder.build();
        final var struct = new Struct(schema);
        for (final var entry : columns.entrySet()) {
            if (entry.getValue().value() != null) {
                struct.put(entry.getKey(), entry.getValue().value());
            }
        }
        return struct;
    }

    private Struct buildSource(String table, Neo4jCdcEvent event) {
        final var source = new Struct(SOURCE_SCHEMA);
        source.put("connector", CONNECTOR_NAME);
        source.put("table", table);
        final var txCommit = event.txCommitTimeMillis();
        if (txCommit != null) {
            source.put("ts_ms", txCommit);
        }
        final var txId = event.txId();
        if (txId != null) {
            source.put("txId", txId);
        }
        return source;
    }

    private String tableName(String entity, String tableOverride) {
        return tableOverride != null ? tableOverride : tableNaming.apply(entity);
    }

    /**
     * Applies the configured {@link FieldMissingBehavior}. {@code fail} aborts by throwing a {@link DataException};
     * {@code warn}/{@code ignore} return a {@link SkipRecordException} carrying the reason, meant to be rethrown by
     * the caller (written as {@code throw onMissingField(...)}). {@link #build(Neo4jCdcEvent)} converts that into a
     * skipped {@link BuildResult}. This class does not log: the reason is surfaced to the SMT, the single logging
     * point, which logs it at the level dictated by {@code field.missing.behavior}.
     */
    private RuntimeException onMissingField(String problem, String action) {
        if (fieldMissingBehavior == FieldMissingBehavior.FAIL) {
            throw new DataException(problem + "; failing record (field.missing.behavior=fail)");
        }
        return new SkipRecordException(problem + "; " + action);
    }

    /**
     * Internal control-flow signal that an event was skipped under {@code field.missing.behavior=warn}/{@code ignore}.
     * Carried out of {@link #buildRecord(Neo4jCdcEvent)} and turned into a skipped {@link BuildResult} by
     * {@link #build(Neo4jCdcEvent)}. Its stack trace is suppressed since it is not an error.
     */
    private static final class SkipRecordException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        SkipRecordException(String reason) {
            super(reason, null, false, false);
        }
    }

    /** The result of a successful conversion: everything the SMT needs to emit a new record. */
    public record EmittedRecord(String table, Schema keySchema, Struct key, Schema valueSchema, Struct value) {
    }

    /**
     * The outcome of {@link #build(Neo4jCdcEvent)}: either the emitted {@code record}, or a {@code skipReason}
     * explaining why the event was skipped (when {@code field.missing.behavior} is {@code warn}/{@code ignore};
     * {@code fail} throws instead). Exactly one is non-null
     */
    public record BuildResult(EmittedRecord record, String skipReason) {

        static BuildResult of(EmittedRecord record) {
            return new BuildResult(record, null);
        }

        static BuildResult skipped(String skipReason) {
            return new BuildResult(null, skipReason);
        }

        public boolean isSkipped() {
            return skipReason != null;
        }
    }
}
