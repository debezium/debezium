/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class Neo4jPropertyTypesFixture {

    /** Schema name used by the connector, matched by callers that need to recognise a wrapped value. */
    public static final String SCHEMA_NAME = "org.neo4j.connectors.kafka.Neo4jPropertyType";

    public static final Schema DURATION_SCHEMA = SchemaBuilder.struct().optional()
            .field("months", Schema.INT64_SCHEMA)
            .field("days", Schema.INT64_SCHEMA)
            .field("seconds", Schema.INT64_SCHEMA)
            .field("nanoseconds", Schema.INT32_SCHEMA)
            .build();

    public static final Schema POINT_SCHEMA = SchemaBuilder.struct().optional()
            .field("dimension", Schema.INT8_SCHEMA)
            .field("srid", Schema.INT32_SCHEMA)
            .field("x", Schema.FLOAT64_SCHEMA)
            .field("y", Schema.FLOAT64_SCHEMA)
            .field("z", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    public static final Schema SCHEMA = SchemaBuilder.struct().name(SCHEMA_NAME).optional()
            .field("type", Schema.STRING_SCHEMA)
            .field("B", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("I64", Schema.OPTIONAL_INT64_SCHEMA)
            .field("F64", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("S", Schema.OPTIONAL_STRING_SCHEMA)
            .field("BA", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("TLD", Schema.OPTIONAL_STRING_SCHEMA)
            .field("TLDT", Schema.OPTIONAL_STRING_SCHEMA)
            .field("TLT", Schema.OPTIONAL_STRING_SCHEMA)
            .field("TZDT", Schema.OPTIONAL_STRING_SCHEMA)
            .field("TOT", Schema.OPTIONAL_STRING_SCHEMA)
            .field("TD", DURATION_SCHEMA)
            .field("SP", POINT_SCHEMA)
            .field("LB", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).optional().build())
            .field("LI64", SchemaBuilder.array(Schema.INT64_SCHEMA).optional().build())
            .field("LF64", SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build())
            .field("LS", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("LTLD", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("LTLDT", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("LTLT", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("LZDT", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("LTOT", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("LTD", SchemaBuilder.array(DURATION_SCHEMA).optional().build())
            .field("LSP", SchemaBuilder.array(POINT_SCHEMA).optional().build())
            .build();

    private Neo4jPropertyTypesFixture() {
    }

    /** A property value of the given union {@code type} with the matching slot set to {@code value}. */
    public static Struct of(String type, Object value) {
        return new Struct(SCHEMA).put("type", type).put(type, value);
    }

    public static boolean isPropertyType(Object value) {
        return value instanceof Struct struct && SCHEMA_NAME.equals(struct.schema().name());
    }

    public static Struct bool(boolean value) {
        return of("B", value);
    }

    public static Struct i64(long value) {
        return of("I64", value);
    }

    public static Struct f64(double value) {
        return of("F64", value);
    }

    public static Struct string(String value) {
        return of("S", value);
    }

    public static Struct bytes(byte[] value) {
        return of("BA", value);
    }

    public static Struct date(String isoDate) {
        return of("TLD", isoDate);
    }

    public static Struct localDateTime(String isoLocalDateTime) {
        return of("TLDT", isoLocalDateTime);
    }

    public static Struct localTime(String isoLocalTime) {
        return of("TLT", isoLocalTime);
    }

    public static Struct zonedDateTime(String isoZonedDateTime) {
        return of("TZDT", isoZonedDateTime);
    }

    public static Struct offsetTime(String isoOffsetTime) {
        return of("TOT", isoOffsetTime);
    }

    public static Struct duration(long months, long days, long seconds, int nanoseconds) {
        final var value = new Struct(DURATION_SCHEMA)
                .put("months", months)
                .put("days", days)
                .put("seconds", seconds)
                .put("nanoseconds", nanoseconds);
        return of("TD", value);
    }

    public static Struct point(int srid, double x, double y) {
        final var value = new Struct(POINT_SCHEMA)
                .put("dimension", (byte) 2)
                .put("srid", srid)
                .put("x", x)
                .put("y", y);
        return of("SP", value);
    }

    public static Struct stringList(List<String> values) {
        return of("LS", values);
    }

    public static Struct longList(List<Long> values) {
        return of("LI64", values);
    }
}
