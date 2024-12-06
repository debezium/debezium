/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import io.debezium.config.EnumeratedValue;

/**
 * The set of predefined TemporalPrecisionMode options.
 */
public enum TemporalPrecisionMode implements EnumeratedValue {
    /**
     * Represent time and date values based upon the resolution in the database, using {@link io.debezium.time} semantic
     * types.
     */
    ADAPTIVE("adaptive"),

    /**
     * Represent timestamp, datetime and date values based upon the resolution in the database, using
     * {@link io.debezium.time} semantic types. TIME fields will always be represented as microseconds
     * in INT64 / {@link java.lang.Long} using {@link io.debezium.time.MicroTime}
     */
    ADAPTIVE_TIME_MICROSECONDS("adaptive_time_microseconds"),

    /**
     * Represent timestamp, datetime, date and time values as ISO 8601 string,
     * using {@link io.debezium.time.IsoDate}, {@link io.debezium.time.IsoTime}, {@link io.debezium.time.IsoTimestamp} semantic types.
     */
    ISOSTRING("isostring"),

    /**
     * Represent time and date values using Kafka Connect {@link org.apache.kafka.connect.data} logical types, which always
     * have millisecond precision.
     */
    CONNECT("connect"),

    /**
     * Represent timestamp, datetime, time values using {@link io.debezium.time.MicroTime} semantic type,
     * which always have microseconds precision
     */
    MICROSECONDS("microseconds"),

    /**
     * Represent timestamp, datetime, time values using {@link io.debezium.time.NanoTime} semantic type,
     * which always have nanoseconds precision
     */
    NANOSECONDS("nanoseconds");

    private final String value;

    TemporalPrecisionMode(String value) {
        this.value = value;
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
}
