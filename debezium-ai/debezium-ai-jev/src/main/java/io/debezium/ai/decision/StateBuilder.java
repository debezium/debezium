/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Builds the state sent to the decision service from the configured field paths of the record value.
 * A path is dot separated, e.g. {@code after.description}. A null intermediate struct or a null leaf means the
 * value is missing and it is not part of the state.
 *
 * @author Jiri Pechanec
 */
final class StateBuilder {

    private static final String PATH_SEPARATOR = "\\.";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> paths;
    private final List<List<String>> splitPaths;
    private final StateFormat format;

    StateBuilder(final List<String> paths, final StateFormat format) {
        this.paths = List.copyOf(paths);
        this.splitPaths = this.paths.stream().map(path -> List.of(path.split(PATH_SEPARATOR))).toList();
        this.format = format;
    }

    /**
     * @return the state or {@code null} when none of the configured values is present
     * @throws DataException if a path cannot be resolved or a value has an unsupported type
     */
    Object build(final Struct value) {
        final Map<String, Object> state = new LinkedHashMap<>();
        for (var i = 0; i < paths.size(); i++) {
            final var leaf = resolve(value, paths.get(i), splitPaths.get(i));
            if (leaf != null) {
                state.put(paths.get(i), leaf);
            }
        }
        if (state.isEmpty()) {
            return null;
        }
        return switch (format) {
            case JSON -> state;
            case TEXT -> {
                final List<String> lines = new ArrayList<>();
                for (final var entry : state.entrySet()) {
                    lines.add(entry.getValue() instanceof String text ? text : toJson(entry.getValue()));
                }
                yield String.join("\n", lines);
            }
        };
    }

    private static Object resolve(final Struct root, final String path, final List<String> parts) {
        var current = root;
        for (var i = 0; i < parts.size() - 1; i++) {
            final var field = field(current, parts.get(i), path);
            if (field.schema().type() != Schema.Type.STRUCT) {
                throw new DataException("Invalid state field '" + path + "', '" + parts.get(i) + "' is not a struct.");
            }
            current = current.getStruct(parts.get(i));
            if (current == null) {
                return null;
            }
        }
        final var leaf = field(current, parts.getLast(), path);
        final var leafValue = current.get(leaf);
        return leafValue == null ? null : convert(leafValue, leaf.schema(), path);
    }

    private static org.apache.kafka.connect.data.Field field(final Struct struct, final String name, final String path) {
        final var field = struct.schema().field(name);
        if (field == null) {
            throw new DataException("Invalid state field '" + path + "', there is no field '" + name + "'.");
        }
        return field;
    }

    private static String convertLogical(final Object value, final Schema schema) {
        if (schema.name() == null) {
            return null;
        }
        return switch (schema.name()) {
            case Decimal.LOGICAL_NAME -> ((BigDecimal) value).toPlainString();
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME -> LocalDate.ofInstant(((java.util.Date) value).toInstant(), ZoneOffset.UTC).toString();
            case Time.LOGICAL_NAME -> LocalTime.ofNanoOfDay(((java.util.Date) value).getTime() * 1_000_000L).toString();
            case Timestamp.LOGICAL_NAME -> Instant.ofEpochMilli(((java.util.Date) value).getTime()).toString();
            default -> null;
        };
    }

    private static Object convert(final Object value, final Schema schema, final String path) {
        final var logical = convertLogical(value, schema);
        if (logical != null) {
            return logical;
        }
        return switch (schema.type()) {
            case STRING, BOOLEAN, INT8, INT16, INT32, INT64 -> value;
            case FLOAT32, FLOAT64 -> {
                if (!Double.isFinite(((Number) value).doubleValue())) {
                    throw new DataException("Invalid state field '" + path + "', non finite numbers are not supported.");
                }
                yield value;
            }
            case BYTES -> throw new DataException("Invalid state field '" + path + "', bytes are not supported.");
            case STRUCT -> {
                final var struct = (Struct) value;
                final Map<String, Object> result = new LinkedHashMap<>();
                for (final var field : struct.schema().fields()) {
                    final var fieldValue = struct.get(field);
                    if (fieldValue != null) {
                        result.put(field.name(), convert(fieldValue, field.schema(), path + "." + field.name()));
                    }
                }
                yield result;
            }
            case ARRAY -> {
                final List<Object> result = new ArrayList<>();
                for (final var element : (List<?>) value) {
                    result.add(element == null ? null : convert(element, schema.valueSchema(), path));
                }
                yield result;
            }
            case MAP -> {
                final Map<String, Object> result = new LinkedHashMap<>();
                for (final var entry : ((Map<?, ?>) value).entrySet()) {
                    if (!(entry.getKey() instanceof String key)) {
                        throw new DataException("Invalid state field '" + path + "', only maps with string keys are supported.");
                    }
                    if (entry.getValue() != null) {
                        result.put(key, convert(entry.getValue(), schema.valueSchema(), path + "." + key));
                    }
                }
                yield result;
            }
        };
    }

    private static String toJson(final Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new DataException("Unable to convert the state value to text", e);
        }
    }

}
