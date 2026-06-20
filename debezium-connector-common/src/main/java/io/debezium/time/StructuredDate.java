/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalDate;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Structured date semantic type that preserves calendar components without epoch conversion.
 */
public final class StructuredDate {

    public static final String SCHEMA_NAME = "io.debezium.time.StructuredDate";

    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .version(1)
                .field(StructuredTemporal.YEAR_FIELD, StructuredTemporal.optionalInt32())
                .field(StructuredTemporal.MONTH_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.DAY_FIELD, StructuredTemporal.optionalInt8())
                .field(StructuredTemporal.SPECIAL_VALUE_FIELD, StructuredTemporal.optionalString());
    }

    public static Schema schema() {
        return builder().build();
    }

    public static Struct from(LocalDate value) {
        return from(schema(), value);
    }

    public static Struct from(Schema schema, LocalDate value) {
        return from(schema, value.getYear(), value.getMonthValue(), value.getDayOfMonth());
    }

    public static Struct from(Schema schema, int year, int month, int day) {
        return new Struct(schema)
                .put(StructuredTemporal.YEAR_FIELD, year)
                .put(StructuredTemporal.MONTH_FIELD, (byte) month)
                .put(StructuredTemporal.DAY_FIELD, (byte) day);
    }

    public static Struct toStructuredDate(Schema schema, Object value, TemporalAdjuster adjuster) {
        LocalDate date = Conversions.toLocalDate(value);
        if (adjuster != null) {
            date = date.with(adjuster);
        }
        return from(schema, date);
    }

    public static Struct positiveInfinity(Schema schema) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.POSITIVE_INFINITY);
    }

    public static Struct negativeInfinity(Schema schema) {
        return StructuredTemporal.specialValue(schema, StructuredTemporal.NEGATIVE_INFINITY);
    }

    private StructuredDate() {
    }
}
