/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalTime;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured local time semantic type that preserves clock components without epoch conversion.
 */
public final class StructuredTime {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredTime";

    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.HOUR_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.MINUTE_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.SECOND_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.NANOS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32());
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(LocalTime value) {
        return from(schema(), value);
    }

    public static Struct from(Schema schema, LocalTime value) {
        return from(schema, value, -1);
    }

    public static Struct from(Schema schema, LocalTime value, int precision) {
        return StructuredTemporal.withPrecision(new Struct(schema)
                .put(StructuredTemporal.HOUR_FIELD, (byte) value.getHour())
                .put(StructuredTemporal.MINUTE_FIELD, (byte) value.getMinute())
                .put(StructuredTemporal.SECOND_FIELD, (byte) value.getSecond())
                .put(StructuredTemporal.NANOS_FIELD, value.getNano()), precision);
    }

    public static Struct toStructuredTime(Schema schema, Object value, TemporalAdjuster adjuster) {
        return toStructuredTime(schema, value, adjuster, -1);
    }

    public static Struct toStructuredTime(Schema schema, Object value, TemporalAdjuster adjuster, int precision) {
        LocalTime time = Conversions.toLocalTime(value);
        if (adjuster != null) {
            time = time.with(adjuster);
        }
        return from(schema, time, precision);
    }

    private StructuredTime() {
    }
}
