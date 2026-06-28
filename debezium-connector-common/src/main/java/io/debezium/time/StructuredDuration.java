/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured duration semantic type that preserves calendar and clock duration components.
 */
public final class StructuredDuration {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredDuration";

    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.YEARS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MONTHS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.DAYS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.HOURS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MINUTES_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.SECONDS_FIELD, StructuredTemporal.optionalInt64())
                .field(StructuredTemporal.NANOS_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.PRECISION_FIELD, StructuredTemporal.optionalInt32());
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, int nanos) {
        return from(schema, years, months, days, hours, minutes, seconds, nanos, -1);
    }

    public static Struct from(Schema schema, int years, int months, int days, int hours, int minutes, long seconds, int nanos, int precision) {
        return StructuredTemporal.withPrecision(new Struct(schema)
                .put(StructuredTemporal.YEARS_FIELD, years)
                .put(StructuredTemporal.MONTHS_FIELD, months)
                .put(StructuredTemporal.DAYS_FIELD, days)
                .put(StructuredTemporal.HOURS_FIELD, hours)
                .put(StructuredTemporal.MINUTES_FIELD, minutes)
                .put(StructuredTemporal.SECONDS_FIELD, seconds)
                .put(StructuredTemporal.NANOS_FIELD, nanos), precision);
    }

    private StructuredDuration() {
    }
}
