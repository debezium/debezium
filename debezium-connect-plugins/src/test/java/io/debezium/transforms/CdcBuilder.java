/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.transforms.neo4j.Neo4jPropertyTypesFixture;

/**
 * Test-only fluent builder for Neo4j CDC event value {@link Struct}s (CDC source strategy, {@code EXTENDED} payload
 * mode), matching the shape the Neo4j source connector actually emits and that
 * {@link io.debezium.transforms.neo4j.Neo4jCdcEvent} reads.
 * Test values are given as plain Java objects (e.g. {@code "id", 1004L}) and wrapped into {@code Neo4jPropertyType}
 * structs automatically.
 */
final class CdcBuilder {

    private CdcBuilder() {
    }

    static NodeBuilder node(String operation, List<String> labels) {
        return new NodeBuilder(operation, labels);
    }

    static RelationshipBuilder relationship(String operation, String type) {
        return new RelationshipBuilder(operation, type);
    }

    static final class NodeBuilder {
        private final String operation;
        private final List<String> labels;
        private final LinkedHashMap<String, Struct> keyGroups = new LinkedHashMap<>();
        private LinkedHashMap<String, Object> before;
        private LinkedHashMap<String, Object> after;

        NodeBuilder(String operation, List<String> labels) {
            this.operation = operation;
            this.labels = labels;
        }

        NodeBuilder keys(String label, Object... kv) {
            keyGroups.put(label, keyGroup(label, ordered(kv)));
            return this;
        }

        NodeBuilder before(Object... kv) {
            before = ordered(kv);
            return this;
        }

        NodeBuilder after(Object... kv) {
            after = ordered(kv);
            return this;
        }

        Struct build() {
            final LinkedHashMap<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "NODE");
            event.put("operation", wire(operation));
            event.put("labels", labels);
            if (!keyGroups.isEmpty()) {
                event.put("keys", keysByLabel(keyGroups));
            }
            event.put("state", buildState(before, after));
            return root(event);
        }
    }

    static final class RelationshipBuilder {
        private final String operation;
        private final String type;
        private Struct start;
        private Struct end;
        private LinkedHashMap<String, Object> after;

        RelationshipBuilder(String operation, String type) {
            this.operation = operation;
            this.type = type;
        }

        RelationshipBuilder start(String label, Object... kv) {
            start = endpoint(label, kv);
            return this;
        }

        RelationshipBuilder end(String label, Object... kv) {
            end = endpoint(label, kv);
            return this;
        }

        RelationshipBuilder after(Object... kv) {
            after = ordered(kv);
            return this;
        }

        Struct build() {
            final LinkedHashMap<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "RELATIONSHIP");
            event.put("operation", wire(operation));
            event.put("type", type);
            if (start != null) {
                event.put("start", start);
            }
            if (end != null) {
                event.put("end", end);
            }
            event.put("state", buildState(null, after));
            return root(event);
        }

        private static Struct endpoint(String label, Object... kv) {
            final var keys = keysByLabel(Map.of(label, keyGroup(label, ordered(kv))));
            final var schema = SchemaBuilder.struct().name("Node." + label).optional()
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field("keys", keys.schema())
                    .build();
            return new Struct(schema).put("labels", List.of(label)).put("keys", keys);
        }
    }


    /** Wraps the operation code {@code c}/{@code u}/{@code d} into the connector's wire value. */
    private static String wire(String operation) {
        return switch (operation) {
            case "c" -> "CREATE";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            default -> operation;
        };
    }

    private static Struct root(LinkedHashMap<String, Object> event) {
        final var metadataSchema = SchemaBuilder.struct().name("Metadata").optional()
                .field("txCommitTime", Neo4jPropertyTypesFixture.SCHEMA)
                .build();
        final var metadata = new Struct(metadataSchema)
                .put("txCommitTime", Neo4jPropertyTypesFixture.zonedDateTime("2024-01-01T00:00:00Z"));

        final var eventStruct = buildStruct("Event", event);
        final var rootSchema = SchemaBuilder.struct().name("Neo4jCdcValue")
                .field("txId", Schema.INT64_SCHEMA)
                .field("metadata", metadataSchema)
                .field("event", eventStruct.schema())
                .build();
        return new Struct(rootSchema)
                .put("txId", 1L)
                .put("metadata", metadata)
                .put("event", eventStruct);
    }

    private static Struct buildState(LinkedHashMap<String, Object> before, LinkedHashMap<String, Object> after) {
        final LinkedHashMap<String, Object> state = new LinkedHashMap<>();
        if (before != null) {
            state.put("before", buildImage("Before", before));
        }
        if (after != null) {
            state.put("after", buildImage("After", after));
        }
        return buildStruct("State", state);
    }

    /** Builds a {@code {properties: MAP<String, Neo4jPropertyType>}} image struct from name/value pairs. */
    private static Struct buildImage(String name, LinkedHashMap<String, Object> properties) {
        final Map<String, Struct> map = new LinkedHashMap<>();
        for (final var entry : properties.entrySet()) {
            map.put(entry.getKey(), wrap(entry.getValue()));
        }
        final var mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Neo4jPropertyTypesFixture.SCHEMA).build();
        final var imageSchema = SchemaBuilder.struct().name(name).optional().field("properties", mapSchema).build();
        return new Struct(imageSchema).put("properties", map);
    }

    /** Builds a by-label {@code keys} struct: {@code {<label>: [ {keyProp: Neo4jPropertyType, ...} ]}}. */
    private static Struct keysByLabel(Map<String, Struct> keyGroups) {
        final var builder = SchemaBuilder.struct().name("Keys").optional();
        for (final var entry : keyGroups.entrySet()) {
            builder.field(entry.getKey(), SchemaBuilder.array(entry.getValue().schema()).optional().build());
        }
        final var struct = new Struct(builder.build());
        for (final var entry : keyGroups.entrySet()) {
            struct.put(entry.getKey(), List.of(entry.getValue()));
        }
        return struct;
    }

    /** Builds one key-group struct: {@code {keyProp: Neo4jPropertyType, ...}}. */
    private static Struct keyGroup(String label, LinkedHashMap<String, Object> kv) {
        final var builder = SchemaBuilder.struct().name(label + "Key").optional();
        for (final var key : kv.keySet()) {
            builder.field(key, Neo4jPropertyTypesFixture.SCHEMA);
        }
        final var struct = new Struct(builder.build());
        for (final var entry : kv.entrySet()) {
            struct.put(entry.getKey(), wrap(entry.getValue()));
        }
        return struct;
    }

    /** Wraps a plain Java value into a {@code Neo4jPropertyType} struct, or returns it unchanged if already one. */
    private static Struct wrap(Object value) {
        if (Neo4jPropertyTypesFixture.isPropertyType(value)) {
            return (Struct) value;
        }
        if (value instanceof Boolean b) {
            return Neo4jPropertyTypesFixture.bool(b);
        }
        if (value instanceof Long l) {
            return Neo4jPropertyTypesFixture.i64(l);
        }
        if (value instanceof Integer i) {
            return Neo4jPropertyTypesFixture.i64(i.longValue());
        }
        if (value instanceof Double d) {
            return Neo4jPropertyTypesFixture.f64(d);
        }
        if (value instanceof Float f) {
            return Neo4jPropertyTypesFixture.f64(f.doubleValue());
        }
        if (value instanceof String s) {
            return Neo4jPropertyTypesFixture.string(s);
        }
        if (value instanceof byte[] bytes) {
            return Neo4jPropertyTypesFixture.bytes(bytes);
        }
        if (value instanceof List<?> list) {
            return wrapList(list);
        }
        throw new IllegalArgumentException("Unsupported test value type: " + value.getClass());
    }

    @SuppressWarnings("unchecked")
    private static Struct wrapList(List<?> list) {
        final var first = list.isEmpty() ? null : list.get(0);
        if (first instanceof Long) {
            return Neo4jPropertyTypesFixture.longList((List<Long>) list);
        }
        return Neo4jPropertyTypesFixture.stringList((List<String>) list);
    }

    private static LinkedHashMap<String, Object> ordered(Object... kv) {
        if (kv.length % 2 != 0) {
            throw new IllegalArgumentException("Expected key, value pairs but got an odd number of arguments");
        }
        final LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put((String) kv[i], kv[i + 1]);
        }
        return map;
    }

    /** Builds a struct from name/value pairs, inferring each field schema from its (primitive/Struct/List) value. */
    private static Struct buildStruct(String name, LinkedHashMap<String, Object> fields) {
        final SchemaBuilder builder = SchemaBuilder.struct().name(name).optional();
        for (final var entry : fields.entrySet()) {
            builder.field(entry.getKey(), schemaFor(entry.getValue()));
        }
        final Schema schema = builder.build();
        final Struct struct = new Struct(schema);
        for (final var entry : fields.entrySet()) {
            struct.put(entry.getKey(), entry.getValue());
        }
        return struct;
    }

    private static Schema schemaFor(Object value) {
        if (value instanceof String) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
        if (value instanceof Struct struct) {
            return struct.schema();
        }
        if (value instanceof List<?> list) {
            final Schema element = list.isEmpty() ? Schema.OPTIONAL_STRING_SCHEMA : schemaFor(list.get(0));
            return SchemaBuilder.array(element).optional().build();
        }
        throw new IllegalArgumentException("Unsupported test value type: " + value.getClass());
    }
}
