/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java timestamp representations into a UTC ISO 8601 string representation
 * (including date, time, fractional parts, and offset from UTC), and for defining a Kafka Connect
 * {@link Schema} for timestamp values in this format.
 * <p>
 * The ISO 8601 date-time format includes the date, time (including fractional parts), and offset from UTC, such as
 * '2011-12-03T10:15:30Z'.
 *
 * @author Ismail Simsek
 * @see Timestamp
 * @see MicroTimestamp
 * @see NanoTimestamp
 * @see ZonedTime
 */
public class IsoTimestamp {

    public static final String SCHEMA_NAME = "io.debezium.time.IsoTimestamp";
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    /**
     * Returns a {@link SchemaBuilder} for a {@link IsoTimestamp}. The builder will create a schema that describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string() STRING} for the literal
     * type storing the timestamp in UTC ISO 8601 format (e.g., "2023-11-21T18:29:26Z").
     * <p>
     * You can use the resulting SchemaBuilder to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link IsoTimestamp} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string() STRING} for the literal() STRING} for the literal
     * type storing the timestamp in UTC ISO 8601 format (e.g., "2023-11-21T18:29:26Z").
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Converts a {@link java.time.Instant} (represented as milliseconds since epoch), {@link java.time.LocalDateTime},
     * {@link java.util.Date}, {@link java.sql.Timestamp}, or any other object convertible to a {@link LocalDateTime}
     * to its UTC ISO 8601 string representation (e.g., "2011-12-03T10:15:30Z").
     * <p>
     * This method handles various timestamp-related objects. For {@link Instant}, it converts the milliseconds since
     * epoch to a {@link LocalDateTime} in UTC. For other objects, it uses the {@link Conversions#toLocalDateTime(Object)}
     * method for conversion. If an adjuster is provided, it's used to adjust the resulting {@link LocalDateTime} before
     * formatting it to the string representation.
     *
     * @param value the timestamp value; may not be null
     * @param adjuster the optional component that adjusts the local date-time value before obtaining the string representation;
     * may be null if no adjustment is necessary
     * @return the UTC ISO 8601 string representation of the timestamp
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types
     */
    public static String toIsoString(Object value, TemporalAdjuster adjuster) {
        LocalDateTime dateTime;
        if (value instanceof Long) {
            dateTime = Instant.ofEpochMilli((long) value).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        else {
            dateTime = Conversions.toLocalDateTime(value);
        }

        if (adjuster != null) {
            dateTime = dateTime.with(adjuster);
        }

        return dateTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
    }

    private IsoTimestamp() {
    }
}