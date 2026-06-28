/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured local timestamp semantic type that preserves calendar and clock components without epoch conversion.
 */
public final class StructuredTimestamp {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredTimestamp";

    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.YEAR_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MONTH_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.DAY_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.HOUR_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.MINUTE_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.SECOND_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.NANOS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.SPECIAL_VALUE_FIELD, StructuredTemporal.optionalString())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32());
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(LocalDateTime value) {
        return from(schema(), value);
    }

    public static Struct from(Schema schema, LocalDateTime value) {
        return from(schema, value, -1);
    }

    public static Struct from(Schema schema, LocalDateTime value, int precision) {
        return from(schema, value.getYear(), value.getMonthValue(), value.getDayOfMonth(), value.getHour(), value.getMinute(), value.getSecond(), value.getNano(),
                precision);
    }

    public static Struct from(Schema schema, int year, int month, int day, int hour, int minute, int second, int nanos) {
        return from(schema, year, month, day, hour, minute, second, nanos, -1);
    }

    public static Struct from(Schema schema, int year, int month, int day, int hour, int minute, int second, int nanos, int precision) {
        return StructuredTemporal.withPrecision(new Struct(schema)
                .put(StructuredTemporal.YEAR_FIELD, year)
                .put(StructuredTemporal.MONTH_FIELD, (byte) month)
                .put(StructuredTemporal.DAY_FIELD, (byte) day)
                .put(StructuredTemporal.HOUR_FIELD, (byte) hour)
                .put(StructuredTemporal.MINUTE_FIELD, (byte) minute)
                .put(StructuredTemporal.SECOND_FIELD, (byte) second)
                .put(StructuredTemporal.NANOS_FIELD, nanos), precision);
    }

    public static Struct toStructuredTimestamp(Schema schema, Object value, TemporalAdjuster adjuster) {
        return toStructuredTimestamp(schema, value, adjuster, -1);
    }

    public static Struct toStructuredTimestamp(Schema schema, Object value, TemporalAdjuster adjuster, int precision) {
        LocalDateTime timestamp = Conversions.toLocalDateTime(value);
        if (adjuster != null) {
            timestamp = timestamp.with(adjuster);
        }
        return from(schema, timestamp, precision);
    }

    public static Struct positiveInfinity(Schema schema) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.POSITIVE_INFINITY);
    }

    public static Struct positiveInfinity(Schema schema, int precision) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.POSITIVE_INFINITY, precision);
    }

    public static Struct negativeInfinity(Schema schema) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.NEGATIVE_INFINITY);
    }

    public static Struct negativeInfinity(Schema schema, int precision) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.NEGATIVE_INFINITY, precision);
    }

    private StructuredTimestamp() {
    }
}
