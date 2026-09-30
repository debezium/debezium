/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

/**
 * A view over an incoming Neo4j CDC change event {@link Struct} (Neo4j source connector, CDC source strategy,
 * {@code EXTENDED} payload mode).
 * <p>
 * This class isolates <em>all</em> knowledge of the Neo4j CDC wire layout. The relevant shape, as emitted by the
 * connector's {@code ChangeEventConverter}, is:
 * <ul>
 * <li>the record root is {@code {id, txId, seq, metadata, event}}; {@code txId} lives at the root (not in
 * {@code metadata}), and {@code metadata.txCommitTime} is itself a {@code Neo4jPropertyType} (a zoned datetime);</li>
 * <li>{@code event} is {@code {elementId, eventType, operation, labels, keys, state}} for a node and
 * {@code {elementId, eventType, operation, type, start, end, keys, state}} for a relationship;</li>
 * <li>{@code eventType} is {@code "NODE"} / {@code "RELATIONSHIP"} and {@code operation} is {@code "CREATE"} /
 * {@code "UPDATE"} / {@code "DELETE"};</li>
 * <li>a node's {@code keys} is a struct keyed by label, each value an <em>array</em> of key-groups (a struct of key
 * property to value); an endpoint ({@code start}/{@code end}) is a node struct {@code {elementId, labels, keys}} with
 * the same by-label key shape;</li>
 * <li>{@code state.before}/{@code state.after} carry {@code {labels, properties}}, where {@code properties} is a
 * {@code MAP} from property name to a {@code Neo4jPropertyType} union struct.</li>
 * </ul>
 * If a real connector's field paths differ, only this class (and {@link Neo4jTypeMapper} for the property union) need
 * to change.
 */
public class Neo4jCdcEvent {

    public static final String NODE = "NODE";
    public static final String RELATIONSHIP = "RELATIONSHIP";

    private final Struct root;
    private final Struct event;
    private final Struct metadata;

    private Neo4jCdcEvent(Struct root, Struct event, Struct metadata) {
        this.root = root;
        this.event = event;
        this.metadata = metadata;
    }

    /**
     * The outcome of {@link #from(Object)}: either a {@link #recognized() recognized} CDC {@code event}, or a
     * {@code skipReason} explaining why the record is not a Neo4j CDC event and should be passed through unchanged.
     * Exactly one of {@code event} / {@code skipReason} is non-null.
     */
    public record Result(Neo4jCdcEvent event, String skipReason) {

        static Result recognized(Neo4jCdcEvent event) {
            return new Result(event, null);
        }

        static Result notRecognized(String skipReason) {
            return new Result(null, skipReason);
        }

        public boolean recognized() {
            return event != null;
        }
    }

    /**
     * Inspects the given record value and returns a {@link Result}: a recognized CDC event, or a reason explaining
     * why the value is not a Neo4j CDC event (so the caller can pass the record through unchanged and log why).
     */
    public static Result from(Object value) {
        if (!(value instanceof Struct root)) {
            return Result.notRecognized(String.format(
                    "record value is not a Struct (type=%s); the SMT requires a schema-typed CDC Struct "
                            + "(EXTENDED payload mode)",
                    type(value)));
        }
        final var event = optStruct(root, "event");
        if (event == null) {
            return Result.notRecognized(String.format(
                    "record value Struct (schema=%s) has no 'event' field, so it is not a Neo4j CDC change event "
                            + "(is the Neo4j source connector emitting EXTENDED-payload CDC?)",
                    schemaName(root)));
        }
        return Result.recognized(new Neo4jCdcEvent(root, event, optStruct(root, "metadata")));
    }

    private static String type(Object value) {
        return value == null ? "undefined" : value.getClass().getName();
    }

    private static String schemaName(Struct struct) {
        final var name = struct.schema().name();
        return name == null ? "<anonymous>" : name;
    }

    public String eventType() {
        return optString(event, "eventType");
    }

    public boolean isNode() {
        return NODE.equals(eventType());
    }

    public boolean isRelationship() {
        return RELATIONSHIP.equals(eventType());
    }

    /**
     * The Neo4j CDC operation, mapped to a Debezium operation: {@code CREATE}-&gt;{@code c}, {@code UPDATE}-&gt;
     * {@code u}, {@code DELETE}-&gt;{@code d}. Returns {@code null} for an unrecognized or absent operation.
     */
    public Operation operation() {
        return Operation.fromWire(optString(event, "operation"));
    }

    public List<String> labels() {
        return optStringList(event, "labels");
    }

    public String type() {
        return optString(event, "type");
    }

    public Endpoint start() {
        final var s = optStruct(event, "start");
        return s == null ? null : new Endpoint(s);
    }

    public Endpoint end() {
        final var s = optStruct(event, "end");
        return s == null ? null : new Endpoint(s);
    }

    /**
     * The node key columns for a given label: the first key-group of the {@code keys.<Label>} array, as a map from
     * key-property name to its {@code Neo4jPropertyType} value struct. {@code null} when the label has no key entry
     * (no Neo4j key/uniqueness constraint on that label).
     */
    public Map<String, Struct> keysFor(String label) {
        return firstKeyGroup(optStruct(event, "keys"), label);
    }

    /** The {@code state.after.properties} map (property name -&gt; value struct), empty when absent. */
    public Map<String, Struct> afterProperties() {
        return stateProperties("after");
    }

    /** The {@code state.before.properties} map (property name -&gt; value struct), empty when absent. */
    public Map<String, Struct> beforeProperties() {
        return stateProperties("before");
    }

    private Map<String, Struct> stateProperties(String image) {
        final var state = optStruct(event, "state");
        if (state == null) {
            return Collections.emptyMap();
        }
        final var img = optStruct(state, image);
        return img == null ? Collections.emptyMap() : optPropertyMap(img, "properties");
    }

    /** The source transaction id (from the record root), or {@code null} when absent. */
    public Long txId() {
        if (root == null || root.schema().field("txId") == null) {
            return null;
        }
        final var value = root.get("txId");
        return value == null ? null : ((Number) value).longValue();
    }

    /**
     * The source transaction commit time as epoch milliseconds. In EXTENDED mode {@code metadata.txCommitTime} is a
     * {@code Neo4jPropertyType} holding a zoned datetime ({@code TZDT}) ISO string. Returns {@code null} when absent
     * or unparseable.
     */
    public Long txCommitTimeMillis() {
        final var commit = metadata == null ? null : optStruct(metadata, "txCommitTime");
        final var raw = commit == null ? null : optString(commit, "TZDT");
        if (raw == null) {
            return null;
        }
        try {
            return Instant.parse(raw).toEpochMilli();
        }
        catch (DateTimeParseException e) {
            return null;
        }
    }

    /** A relationship endpoint ({@code start} / {@code end}): its labels and per-label key columns. */
    public static class Endpoint {
        private final Struct struct;

        Endpoint(Struct struct) {
            this.struct = struct;
        }

        public List<String> labels() {
            return optStringList(struct, "labels");
        }

        public Map<String, Struct> keysFor(String label) {
            return firstKeyGroup(optStruct(struct, "keys"), label);
        }
    }

    public enum Operation {
        CREATE("CREATE", "c"),
        UPDATE("UPDATE", "u"),
        DELETE("DELETE", "d");

        private final String wireName;
        private final String code;

        Operation(String wireName, String code) {
            this.wireName = wireName;
            this.code = code;
        }

        public String code() {
            return code;
        }

        public boolean isCreate() {
            return this == CREATE;
        }

        public boolean isUpdate() {
            return this == UPDATE;
        }

        public boolean isDelete() {
            return this == DELETE;
        }

        static Operation fromWire(String wireName) {
            if (wireName == null) {
                return null;
            }
            for (final var op : values()) {
                if (op.wireName.equals(wireName)) {
                    return op;
                }
            }
            return null;
        }
    }

    /**
     * Reads the first key-group of a by-label {@code keys} struct: {@code keys.<label>} is an array of key-groups
     * (each a struct of key-property to {@code Neo4jPropertyType} value); the first group is the record's key.
     * Returns {@code null} when the label has no populated key entry.
     */
    private static Map<String, Struct> firstKeyGroup(Struct keys, String label) {
        if (keys == null || keys.schema().field(label) == null) {
            return null;
        }
        final var value = keys.get(label);
        if (!(value instanceof List<?> groups) || groups.isEmpty() || !(groups.get(0) instanceof Struct group)) {
            return null;
        }
        final Map<String, Struct> columns = new LinkedHashMap<>();
        for (final var field : group.schema().fields()) {
            if (group.get(field) instanceof Struct propertyType) {
                columns.put(field.name(), propertyType);
            }
        }
        return columns;
    }

    private static Struct optStruct(Struct struct, String field) {
        if (struct == null || struct.schema().field(field) == null) {
            return null;
        }
        final var value = struct.get(field);
        return value instanceof Struct s ? s : null;
    }

    private static String optString(Struct struct, String field) {
        if (struct == null || struct.schema().field(field) == null) {
            return null;
        }
        final var value = struct.get(field);
        return value == null ? null : value.toString();
    }

    @SuppressWarnings("unchecked")
    private static List<String> optStringList(Struct struct, String field) {
        if (struct == null || struct.schema().field(field) == null) {
            return Collections.emptyList();
        }
        final var value = struct.get(field);
        if (value instanceof List<?> list) {
            return (List<String>) list;
        }
        return Collections.emptyList();
    }

    /** Reads a {@code MAP<String, Neo4jPropertyType>} field into an ordered map of property name to value struct. */
    private static Map<String, Struct> optPropertyMap(Struct struct, String field) {
        if (struct == null || struct.schema().field(field) == null) {
            return Collections.emptyMap();
        }
        if (!(struct.get(field) instanceof Map<?, ?> map)) {
            return Collections.emptyMap();
        }
        final Map<String, Struct> properties = new LinkedHashMap<>();
        for (final var entry : map.entrySet()) {
            if (entry.getValue() instanceof Struct propertyType) {
                properties.put(String.valueOf(entry.getKey()), propertyType);
            }
        }
        return properties;
    }

}
