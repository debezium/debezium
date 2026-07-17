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
 * Shared constants and helpers for structured temporal semantic types.
 */
public final class StructuredTemporal {

    public static final String YEAR_FIELD = "year";
    public static final String MONTH_FIELD = "month";
    public static final String DAY_FIELD = "day";
    public static final String HOUR_FIELD = "hour";
    public static final String MINUTE_FIELD = "minute";
    public static final String SECOND_FIELD = "second";
    public static final String PICOSECONDS_FIELD = "picoseconds";
    public static final String OFFSET_SECONDS_FIELD = "offsetSeconds";
    public static final String ZONE_ID_FIELD = "zoneId";
    public static final String SPECIAL_VALUE_FIELD = "specialValue";
    public static final String PRECISION_FIELD = "precision";

    /**
     * Schema parameter that describes the source column's fractional-second precision.
     */
    public static final String PRECISION_PARAMETER_KEY = "io.debezium.time.precision";

    /**
     * Schema parameter that describes the semantic kind of a structured duration.
     */
    public static final String DURATION_KIND_PARAMETER_KEY = "io.debezium.time.duration.kind";

    public static final String YEARS_FIELD = "years";
    public static final String MONTHS_FIELD = "months";
    public static final String DAYS_FIELD = "days";
    public static final String HOURS_FIELD = "hours";
    public static final String MINUTES_FIELD = "minutes";
    public static final String SECONDS_FIELD = "seconds";

    public static final String POSITIVE_INFINITY = "POSITIVE_INFINITY";
    public static final String NEGATIVE_INFINITY = "NEGATIVE_INFINITY";

    public static final long PICOSECONDS_PER_NANOSECOND = 1_000L;
    public static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;

    static Schema optionalInt8() {
        return SchemaBuilder.int8().optional().build();
    }

    static Schema optionalInt16() {
        return SchemaBuilder.int16().optional().build();
    }

    static Schema optionalInt32() {
        return SchemaBuilder.int32().optional().build();
    }

    static Schema optionalInt64() {
        return SchemaBuilder.int64().optional().build();
    }

    static Schema optionalString() {
        return SchemaBuilder.string().optional().build();
    }

    static Struct specialValue(Schema schema, String value) {
        return specialValue(schema, value, -1);
    }

    static Struct specialValue(Schema schema, String value, int precision) {
        return withPrecision(new Struct(schema).put(SPECIAL_VALUE_FIELD, value), precision);
    }

    static Struct withPrecision(Struct struct, int precision) {
        if (precision >= 0) {
            struct.put(PRECISION_FIELD, precision);
        }
        return struct;
    }

    static SchemaBuilder withPrecision(SchemaBuilder builder, int precision) {
        if (precision >= 0) {
            builder.parameter(PRECISION_PARAMETER_KEY, Integer.toString(precision));
        }
        return builder;
    }

    static long picosecondsFromNanoseconds(int nanoseconds) {
        return nanoseconds * PICOSECONDS_PER_NANOSECOND;
    }

    public static boolean isPositiveInfinity(Struct value) {
        return isSpecialValue(value, POSITIVE_INFINITY);
    }

    public static boolean isNegativeInfinity(Struct value) {
        return isSpecialValue(value, NEGATIVE_INFINITY);
    }

    public static boolean isSpecialValue(Struct value, String specialValue) {
        return value != null && value.schema().field(SPECIAL_VALUE_FIELD) != null && specialValue.equals(value.getString(SPECIAL_VALUE_FIELD));
    }

    public static boolean isFinite(Struct value) {
        return value != null && (value.schema().field(SPECIAL_VALUE_FIELD) == null || value.getString(SPECIAL_VALUE_FIELD) == null);
    }

    private StructuredTemporal() {
    }
}
