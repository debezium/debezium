/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.EnumeratedValue;
import io.debezium.time.Date;
import io.debezium.time.IsoDate;
import io.debezium.time.IsoTime;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;

/**
 * The set of predefined TemporalPrecisionMode options.
 */
public enum TemporalPrecisionMode implements EnumeratedValue {
    /**
     * Represent time and date values based upon the resolution in the database, using {@link io.debezium.time} semantic
     * types.
     */
    ADAPTIVE("adaptive", Date::builder,
            x -> (x <= 3) ? Time.builder() : (x <= 6) ? MicroTime.builder() : NanoTime.builder(),
            x -> (x <= 3) ? Timestamp.builder() : (x <= 6) ? MicroTimestamp.builder() : NanoTimestamp.builder()),

    /**
     * Represent timestamp, datetime and date values based upon the resolution in the database, using
     * {@link io.debezium.time} semantic types. TIME fields will always be represented as microseconds
     * in INT64 / {@link java.lang.Long} using {@link io.debezium.time.MicroTime}
     */
    ADAPTIVE_TIME_MICROSECONDS("adaptive_time_microseconds", Date::builder,
            x -> MicroTime.builder(),
            x -> (x <= 3) ? Timestamp.builder() : (x <= 6) ? MicroTimestamp.builder() : NanoTimestamp.builder()),

    /**
     * Represent timestamp, datetime, date and time values as ISO 8601 string,
     * using {@link io.debezium.time.IsoDate}, {@link io.debezium.time.IsoTime}, {@link io.debezium.time.IsoTimestamp} semantic types.
     */
    ISOSTRING("isostring", IsoDate::builder, x -> IsoTime.builder(), x -> IsoTimestamp.builder()),

    /**
     * Represent time and date values using Kafka Connect {@link org.apache.kafka.connect.data} logical types, which always
     * have millisecond precision.
     */
    CONNECT("connect", org.apache.kafka.connect.data.Date::builder,
            x -> org.apache.kafka.connect.data.Time.builder(),
            x -> org.apache.kafka.connect.data.Timestamp.builder()),

    /**
     * Represent timestamp, datetime, time values using {@link io.debezium.time.MicroTime} semantic type,
     * which always have microseconds precision
     */
    MICROSECONDS("microseconds", Date::builder, x -> MicroTime.builder(), x -> MicroTimestamp.builder()),

    /**
     * Represent timestamp, datetime, time values using {@link io.debezium.time.NanoTime} semantic type,
     * which always have nanoseconds precision
     */
    NANOSECONDS("nanoseconds", Date::builder, x -> NanoTime.builder(), x -> NanoTimestamp.builder());

    private final String value;
    private final Supplier<SchemaBuilder> dateBuilder;
    private final Function<Integer, SchemaBuilder> timeBuilder;
    private final Function<Integer, SchemaBuilder> timestampBuilder;

    TemporalPrecisionMode(String value, Supplier<SchemaBuilder> dateBuilder,
                          Function<Integer, SchemaBuilder> timeBuilder, Function<Integer, SchemaBuilder> timestampBuilder) {
        this.value = value;
        this.dateBuilder = dateBuilder;
        this.timeBuilder = timeBuilder;
        this.timestampBuilder = timestampBuilder;
    }

    @Override
    public String getValue() {
        return value;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @return the matching option, or null if no match is found
     */
    public static TemporalPrecisionMode parse(String value) {
        if (value == null) {
            return null;
        }
        value = value.trim();
        for (TemporalPrecisionMode option : TemporalPrecisionMode.values()) {
            if (option.getValue().equalsIgnoreCase(value)) {
                return option;
            }
        }
        return null;
    }

    /**
     * Determine if the supplied value is one of the predefined options.
     *
     * @param value the configuration property value; may not be null
     * @param defaultValue the default value; may be null
     * @return the matching option, or null if no match is found and the non-null default is invalid
     */
    public static TemporalPrecisionMode parse(String value, String defaultValue) {
        TemporalPrecisionMode mode = parse(value);
        if (mode == null && defaultValue != null) {
            mode = parse(defaultValue);
        }
        return mode;
    }

    public SchemaBuilder getDateBuilder() {
        return dateBuilder.get();
    }

    public SchemaBuilder getTimeBuilder(int precision) {
        return timeBuilder.apply(precision);
    }

    public SchemaBuilder getTimestampBuilder(int precision) {
        return timestampBuilder.apply(precision);
    }
}
