/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured time-with-zone semantic type that preserves clock components and UTC offset.
 */
public final class StructuredZonedTime {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredZonedTime";

    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.HOUR_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.MINUTE_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.SECOND_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.NANOS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.OFFSET_SECONDS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32());
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(OffsetTime value) {
        return from(schema(), value);
    }

    public static Struct from(Schema schema, OffsetTime value) {
        return from(schema, value, -1);
    }

    public static Struct from(Schema schema, OffsetTime value, int precision) {
        return from(schema, value.getHour(), value.getMinute(), value.getSecond(), value.getNano(),
                value.getOffset().getTotalSeconds(), precision);
    }

    /**
     * Builds the struct directly from raw clock components and a UTC offset, bypassing {@link OffsetTime}.
     * This preserves the source offset as-is (no UTC normalization) and allows the PostgreSQL end-of-day
     * boundary hour {@code 24}, which {@link OffsetTime}/{@link LocalTime} cannot represent.
     */
    public static Struct from(Schema schema, int hour, int minute, int second, int nanos, int offsetSeconds, int precision) {
        return StructuredTemporal.withPrecision(new Struct(schema)
                .put(StructuredTemporal.HOUR_FIELD, (byte) hour)
                .put(StructuredTemporal.MINUTE_FIELD, (byte) minute)
                .put(StructuredTemporal.SECOND_FIELD, (byte) second)
                .put(StructuredTemporal.NANOS_FIELD, nanos)
                .put(StructuredTemporal.OFFSET_SECONDS_FIELD, offsetSeconds), precision);
    }

    public static Struct toStructuredZonedTime(Schema schema, Object value, ZoneOffset defaultOffset, TemporalAdjuster adjuster) {
        return toStructuredZonedTime(schema, value, defaultOffset, adjuster, -1);
    }

    public static Struct toStructuredZonedTime(Schema schema, Object value, ZoneOffset defaultOffset, TemporalAdjuster adjuster, int precision) {
        OffsetTime time;
        if (value instanceof OffsetTime) {
            time = (OffsetTime) value;
        }
        else if (value instanceof OffsetDateTime) {
            time = ((OffsetDateTime) value).toOffsetTime();
        }
        else {
            final LocalTime localTime = Conversions.toLocalTime(value);
            time = OffsetTime.of(localTime, defaultOffset);
        }
        if (adjuster != null) {
            time = time.with(adjuster);
        }
        return from(schema, time, precision);
    }

    private StructuredZonedTime() {
    }
}
