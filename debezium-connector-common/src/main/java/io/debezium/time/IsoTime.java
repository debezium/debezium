/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into a UTC ISO 8601 string representation
 * focusing on time (including fractional parts and offset from UTC), and for defining a Kafka Connect
 * {@link Schema} for time values in this format.
 * <p>
 * The ISO 8601 time format includes the time (including fractional parts) and offset from UTC, such as '10:15:30Z'.
 *
 * @author Ismail Simsek
 * @see Date
 * @see Time
 * @see Timestamp
 * @see ZonedTimestamp
 */
public class IsoTime {

    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_TIME;
    public static final String SCHEMA_NAME = "io.debezium.time.IsoTime";
    private static final Duration ONE_DAY = Duration.ofDays(1);

    /**
     * Returns a {@link SchemaBuilder} for a {@link IsoTime}. The builder will create a schema that describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#STRING() STRING} for the literal
     * type storing the time in UTC ISO 8601 format (e.g., "10:15:30Z").
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
     * Returns a Schema for a {@link IsoTime} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string() STRING} for the literal
     * type storing the time in UTC ISO 8601 format (e.g., "10:15:30Z").
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Converts a {@link Duration}, {@link java.time.LocalTime}, {@link java.util.Date}, {@link java.sql.Time},
     * or {@link java.sql.Timestamp} object to its UTC ISO 8601 string representation (e.g., "10:15:30Z").
     * <p>
     * This method handles both {@link Duration} values and time-related objects. For {@link Duration} values,
     * it ensures the resulting time is within the valid range (between 00:00:00 and 24:00:00 inclusive)
     * by throwing an {@link IllegalArgumentException} if the duration goes beyond this range and the `acceptLargeValues`
     * parameter is set to false. For time-related objects, it converts them to {@link LocalTime} before formatting.
     *
     * @param value the time or duration value; may not be null
     * @param acceptLargeValues whether to accept {@link Duration} values exceeding the valid range (00:00:00 - 24:00:00)
     * @return the UTC ISO 8601 string representation of the time
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types, or if a
     * {@link Duration} value exceeds the valid range and `acceptLargeValues` is false.
     */
    public static String toIsoString(Object value, boolean acceptLargeValues) {
        if (value instanceof Duration) {
            Duration duration = (Duration) value;
            if (!acceptLargeValues && (duration.isNegative() || duration.compareTo(ONE_DAY) > 0)) {
                throw new IllegalArgumentException("Time values must be between 00:00:00 and 24:00:00 (inclusive): " + duration);
            }
            // Base LocalTime (e.g., 0:00 AM)
            // Calculate the new LocalTime by adding the duration to the base time
            LocalTime localTime = LocalTime.of(0, 0).plus(duration);
            return localTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
        }

        LocalTime localTime = Conversions.toLocalTime(value);
        return localTime.atOffset(ZoneOffset.UTC).format(FORMATTER);
    }

    private IsoTime() {
    }
}
