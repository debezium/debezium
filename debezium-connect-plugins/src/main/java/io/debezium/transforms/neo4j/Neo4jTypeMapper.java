/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

/**
 * Maps a single Neo4j CDC property value to a flat relational column (an output {@link Schema} plus the converted
 * value) that the Debezium JDBC sink can persist.
 * <p>
 * In EXTENDED payload mode the Neo4j source connector wraps every property value in a tagged-union {@link Struct}
 * named {@code org.neo4j.connectors.kafka.Neo4jPropertyType}: a {@code type} field naming the kind ({@code "I64"} =
 * long, {@code "S"} = String, {@code "TLDT"} = local date-time, {@code "SP"} = spatial point, ...) plus one matching
 * typed slot holding the value, with every other slot {@code null}. This mapper switches on the {@code type}
 * discriminator, reads that one populated slot, and maps it to the Kafka Connect / Debezium type the JDBC sink
 * expects: primitives by identity, Neo4j temporals to {@code io.debezium.time.*} logical types, and
 * duration/point/structured lists (which have no portable relational column) to a JSON {@code STRING}.
 */
public final class Neo4jTypeMapper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** The discriminator field of the {@code Neo4jPropertyType} union. */
    static final String TYPE = "type";

    private Neo4jTypeMapper() {
    }

    /** A converted column: the relational-friendly output schema and the value to store under it. */
    public record Converted(Schema schema, Object value) {
    }

    /**
     * Converts one {@code Neo4jPropertyType} union struct (an EXTENDED-payload property value) into a relational
     * column. The struct's {@code type} slot selects both the output schema and how the value is read.
     */
    public static Converted convert(Struct propertyType) {
        final var type = propertyType.getString(TYPE);
        if (type == null) {
            throw new ConnectException("Neo4j property value is missing its '" + TYPE
                    + "' discriminator; not a Neo4jPropertyType union struct: " + propertyType.schema().name());
        }
        return switch (type) {
            case "B" -> new Converted(Schema.OPTIONAL_BOOLEAN_SCHEMA, propertyType.get("B"));
            case "I64" -> new Converted(Schema.OPTIONAL_INT64_SCHEMA, propertyType.get("I64"));
            case "F64" -> new Converted(Schema.OPTIONAL_FLOAT64_SCHEMA, propertyType.get("F64"));
            case "S" -> new Converted(Schema.OPTIONAL_STRING_SCHEMA, propertyType.get("S"));
            case "BA" -> new Converted(Schema.OPTIONAL_BYTES_SCHEMA, propertyType.get("BA"));
            case "TLD" -> new Converted(Date.builder().optional().build(), epochDay(propertyType.getString("TLD")));
            case "TLDT" -> new Converted(Timestamp.builder().optional().build(), epochMillis(propertyType.getString("TLDT")));
            case "TLT" -> new Converted(MicroTime.builder().optional().build(), microOfDay(propertyType.getString("TLT")));
            case "TZDT" -> new Converted(ZonedTimestamp.builder().optional().build(), propertyType.getString("TZDT"));
            case "TOT" -> new Converted(ZonedTime.builder().optional().build(), propertyType.getString("TOT"));
            case "TD" -> new Converted(Schema.OPTIONAL_STRING_SCHEMA, durationToIso(propertyType.getStruct("TD")));
            case "SP" -> new Converted(Schema.OPTIONAL_STRING_SCHEMA, pointToJson(propertyType.getStruct("SP")));
            case "LB", "LI64", "LF64", "LS" -> primitiveArray(propertyType, type);
            // Lists of durations/points render their elements the same way as their scalar forms (ISO string /
            // {srid,x,y[,z]}), so a one-element list matches the corresponding scalar column value.
            case "LTD" -> new Converted(Schema.OPTIONAL_STRING_SCHEMA,
                    toJson(structListToPlain(propertyType.get("LTD"), Neo4jTypeMapper::durationToIso)));
            case "LSP" -> new Converted(Schema.OPTIONAL_STRING_SCHEMA,
                    toJson(structListToPlain(propertyType.get("LSP"), Neo4jTypeMapper::pointToMap)));
            case "LTLD", "LTLDT", "LTLT", "LZDT", "LTOT" ->
                new Converted(Schema.OPTIONAL_STRING_SCHEMA, toJson(propertyType.get(type)));
            default -> new Converted(Schema.OPTIONAL_STRING_SCHEMA, toJson(propertyType.get(type)));
        };
    }

    /** A homogeneous list of primitives maps to an array column, preserving the element type. */
    private static Converted primitiveArray(Struct propertyType, String slot) {
        final var slotSchema = propertyType.schema().field(slot).schema();
        return new Converted(optional(slotSchema), propertyType.get(slot));
    }

    private static Integer epochDay(String isoDate) {
        return isoDate == null ? null : (int) LocalDate.parse(isoDate).toEpochDay();
    }

    private static Long epochMillis(String isoLocalDateTime) {
        return isoLocalDateTime == null ? null : LocalDateTime.parse(isoLocalDateTime).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static Long microOfDay(String isoLocalTime) {
        return isoLocalTime == null ? null : LocalTime.parse(isoLocalTime).toNanoOfDay() / 1_000L;
    }

    /**
     * Renders a Neo4j duration (months/days/seconds/nanoseconds) as an ISO-8601 duration string. Neo4j keeps the
     * calendar part (months, days) and the time part (seconds, nanoseconds) separate, matching ISO-8601's
     * {@code P<date>T<time>} split, so the two are formatted independently and concatenated.
     */
    private static String durationToIso(Struct duration) {
        if (duration == null) {
            return null;
        }
        final long months = duration.getInt64("months");
        final long days = duration.getInt64("days");
        final long seconds = duration.getInt64("seconds");
        final int nanoseconds = duration.getInt32("nanoseconds");
        final Period period;
        try {
            period = Period.of(0, Math.toIntExact(months), Math.toIntExact(days));
        }
        catch (ArithmeticException e) {
            throw new ConnectException(
                    "Neo4j duration months/days exceed the supported range: months=" + months + ", days=" + days, e);
        }
        final var time = Duration.ofSeconds(seconds, nanoseconds);
        // period.toString() -> "P14M3D"; time.toString() -> "PT4H5M6S"; drop the leading 'P' of the time part.
        return period.toString() + time.toString().substring(1);
    }

    private static String pointToJson(Struct point) {
        return toJson(pointToMap(point));
    }

    /** The plain {srid, x, y[, z]} representation of a Neo4j point, shared by the scalar and list mappings. */
    private static Map<String, Object> pointToMap(Struct point) {
        if (point == null) {
            return null;
        }
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("srid", point.getInt32("srid"));
        map.put("x", point.getFloat64("x"));
        map.put("y", point.getFloat64("y"));
        final var z = point.schema().field("z") == null ? null : point.get("z");
        if (z != null) {
            map.put("z", z);
        }
        return map;
    }

    /** Maps each struct element of a Neo4j list to its scalar plain form via {@code elementMapper}. */
    private static List<Object> structListToPlain(Object list, Function<Struct, Object> elementMapper) {
        if (list == null) {
            return null;
        }
        final List<Object> out = new ArrayList<>();
        for (final Object element : (List<?>) list) {
            out.add(element == null ? null : elementMapper.apply((Struct) element));
        }
        return out;
    }

    /**
     * Returns an optional copy of the given schema, preserving its type, logical name, version, doc and
     * parameters (and, for arrays/maps, the element schemas). Returns the schema unchanged if already optional.
     */
    static Schema optional(Schema schema) {
        return schema.isOptional() ? schema : rebuild(schema, true);
    }

    /**
     * Returns a required (non-optional) copy of the given schema, preserving its type, logical name, version,
     * doc and parameters. Used for primary-key columns, which must not be nullable in the relational target.
     * Returns the schema unchanged if already required.
     */
    static Schema required(Schema schema) {
        return schema.isOptional() ? rebuild(schema, false) : schema;
    }

    private static Schema rebuild(Schema schema, boolean optional) {
        final SchemaBuilder builder = switch (schema.type()) {
            case ARRAY -> SchemaBuilder.array(schema.valueSchema());
            case MAP -> SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
            // A STRUCT would need its fields copied; the plain builder would silently drop them, so fail loudly
            // instead. No caller passes a STRUCT today (structured values are serialized to JSON STRING).
            case STRUCT -> throw new ConnectException(
                    "rebuild() does not support STRUCT schemas (its fields would be dropped): " + schema.name());
            default -> new SchemaBuilder(schema.type());
        };
        if (schema.name() != null) {
            builder.name(schema.name());
        }
        if (schema.version() != null) {
            builder.version(schema.version());
        }
        if (schema.doc() != null) {
            builder.doc(schema.doc());
        }
        if (schema.parameters() != null) {
            builder.parameters(schema.parameters());
        }
        if (optional) {
            builder.optional();
        }
        return builder.build();
    }

    private static String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(toPlain(value));
        }
        catch (JsonProcessingException e) {
            throw new ConnectException("Failed to serialize Neo4j property value to JSON", e);
        }
    }

    private static Object toPlain(Object value) {
        if (value instanceof Struct struct) {
            final Map<String, Object> map = new LinkedHashMap<>();
            for (final var field : struct.schema().fields()) {
                final var fieldValue = struct.get(field);
                if (fieldValue != null) {
                    map.put(field.name(), toPlain(fieldValue));
                }
            }
            return map;
        }
        if (value instanceof Map<?, ?> m) {
            final Map<Object, Object> map = new LinkedHashMap<>();
            for (final var entry : m.entrySet()) {
                map.put(entry.getKey(), toPlain(entry.getValue()));
            }
            return map;
        }
        if (value instanceof java.util.List<?> list) {
            return list.stream().map(Neo4jTypeMapper::toPlain).toList();
        }
        return value;
    }
}
