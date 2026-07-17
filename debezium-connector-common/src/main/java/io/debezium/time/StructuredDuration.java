/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.util.Locale;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured duration semantic type that preserves calendar and clock duration components.
 */
public final class StructuredDuration {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredDuration";

    public enum Kind {
        ELAPSED_TIME("elapsed-time"),
        YEAR_MONTH("year-month"),
        DAY_TIME("day-time"),
        MIXED("mixed");

        private final String value;

        Kind(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Kind parse(String value) {
            if (value == null) {
                return null;
            }
            final String normalized = value.toLowerCase(Locale.ENGLISH);
            for (Kind kind : values()) {
                if (kind.value.equals(normalized)) {
                    return kind;
                }
            }
            return null;
        }
    }

    public static SchemaBuilder builder() {
        return builder(-1, null);
    }

    public static SchemaBuilder builder(int precision, Kind kind) {
        final SchemaBuilder builder = StructuredTemporal.withPrecision(SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.YEARS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MONTHS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.DAYS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.HOURS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MINUTES_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.SECONDS_FIELD, StructuredTemporal.optionalInt64())
                .field(StructuredTemporal.PICOSECONDS_FIELD, StructuredTemporal.optionalInt64())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32()), precision);
        if (kind != null) {
            builder.parameter(StructuredTemporal.DURATION_KIND_PARAMETER_KEY, kind.getValue());
        }
        return builder;
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, int nanos) {
        return from(schema, years, months, days, hours, minutes, seconds, nanos, -1);
    }

    public static Struct from(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, int nanos, int precision) {
        return fromPicoseconds(schema, years, months, days, hours, minutes, seconds, StructuredTemporal.picosecondsFromNanoseconds(nanos), precision);
    }

    public static Struct fromPicoseconds(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, long picoseconds,
                                         int precision) {
        return StructuredTemporal.withPrecision(new Struct(schema)
                .put(StructuredTemporal.YEARS_FIELD, years)
                .put(StructuredTemporal.MONTHS_FIELD, months)
                .put(StructuredTemporal.DAYS_FIELD, days)
                .put(StructuredTemporal.HOURS_FIELD, hours)
                .put(StructuredTemporal.MINUTES_FIELD, minutes)
                .put(StructuredTemporal.SECONDS_FIELD, seconds)
                .put(StructuredTemporal.PICOSECONDS_FIELD, picoseconds), precision);
    }

    private StructuredDuration() {
    }
}
