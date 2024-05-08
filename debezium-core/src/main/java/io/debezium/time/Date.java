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

/**
 * A utility for converting various Java temporal object representations into the signed {@link SchemaBuilder#int32() INT32}
 * number of <em>days</em> since January 1, 1970, at 00:00:00UTC, and for defining a Kafka Connect {@link Schema} for date values
 * with no time or timezone information.
 *
 * @author Randall Hauch
 * @see Timestamp
 * @see ZonedTimestamp
 */
public class Date {

    public static final String SCHEMA_NAME = "io.debezium.time.Date";

    /**
     * Returns a {@link SchemaBuilder} for a {@link Date}. The builder will create a schema that describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int32() INT32} for the literal
     * type storing the number of <em>days</em> since January 1, 1970, at 00:00:00Z.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int32()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link Date} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int32() INT32} for the literal
     * type storing the number of <em>days</em> since January 1, 1970, at 00:00:00Z.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Get the number of epoch days of the given {@link java.time.LocalDateTime}, {@link java.time.LocalDate},
     * {@link java.time.LocalTime}, {@link java.util.Date}, {@link java.sql.Date}, {@link java.sql.Time}, or
     * {@link java.sql.Timestamp}, ignoring any time portions of the supplied value.
     *
     * @param value the local or SQL date, time, or timestamp value; may not be null
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the number of days past epoch
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types
     */
    public static int toEpochDay(Object value, TemporalAdjuster adjuster) {
        LocalDate date = Conversions.toLocalDate(value);
        if (adjuster != null) {
            date = date.with(adjuster);
        }
        return (int) date.toEpochDay();
    }

    private Date() {
    }
}
