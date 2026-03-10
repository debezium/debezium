/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into the signed {@link SchemaBuilder#int64() INT64} number of
 * <em>nanoseconds</em> past epoch, and for defining a Kafka Connect {@link Schema} for timestamp values with no timezone
 * information.
 *
 * @author Randall Hauch
 * @see Timestamp
 * @see MicroTimestamp
 * @see ZonedTimestamp
 */
public class NanoTimestamp {

    public static final String SCHEMA_NAME = "io.debezium.time.NanoTimestamp";

    /**
     * Returns a {@link SchemaBuilder} for a {@link NanoTimestamp}. The resulting schema will describe a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int64() INT64} for the literal
     * type storing the number of <em>nanoseconds</em> past midnight.
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link NanoTimestamp} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#int64() INT64} for the literal
     * type storing the number of <em>nanoseconds</em> past midnight.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Get the number of nanoseconds past epoch of the given {@link java.time.LocalDateTime}, {@link java.time.LocalDate},
     * {@link java.time.LocalTime}, {@link java.util.Date}, {@link java.sql.Date}, {@link java.sql.Time}, or
     * {@link java.sql.Timestamp}.
     *
     * @param value the local or SQL date, time, or timestamp value; may not be null
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the epoch nanoseconds
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types
     */
    public static long toEpochNanos(Object value, TemporalAdjuster adjuster) {
        LocalDateTime dateTime = Conversions.toLocalDateTime(value);
        if (adjuster != null) {
            dateTime = dateTime.with(adjuster);
        }
        return toEpochNanos(dateTime);
    }

    /**
     * Get the number of nanoseconds past epoch of the given {@link LocalDateTime}.
     *
     * @param timestamp the Java timestamp value
     * @return the epoch nanoseconds
     */
    private static long toEpochNanos(LocalDateTime timestamp) {
        long nanoInDay = timestamp.toLocalTime().toNanoOfDay();
        long nanosOfDay = toEpochNanos(timestamp.toLocalDate());
        return nanosOfDay + nanoInDay;
    }

    /**
     * Get the number of nanoseconds past epoch of the given {@link LocalDate}.
     *
     * @param date the Java date value
     * @return the epoch nanoseconds
     */
    private static long toEpochNanos(LocalDate date) {
        long epochDay = date.toEpochDay();
        return epochDay * Conversions.NANOSECONDS_PER_DAY;
    }

    private NanoTimestamp() {
    }
}
