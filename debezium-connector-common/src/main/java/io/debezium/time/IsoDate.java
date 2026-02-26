/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java temporal object representations into a UTC ISO 8601 string representation,
 * specifically focusing on dates (without time or timezone information).
 * Additionally, it defines a Kafka Connect {@link Schema} for representing dates in this format.
 *
 * @author Ismail Simsek
 * @see Timestamp
 * @see ZonedTimestamp
 */
public class IsoDate {
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE;
    public static final String SCHEMA_NAME = "io.debezium.time.IsoDate";

    /**
     * Returns a {@link SchemaBuilder} for a {@link IsoDate}. The builder will create a schema that describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string() STRING} for the literal
     * type storing the date in UTC ISO 8601 format (e.g., "2023-11-21").
     * <p>
     * You can use the resulting SchemaBuilder to set or override additional schema settings such as required/optional, default
     * value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(SCHEMA_NAME)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link IsoDate} but with all other default Schema settings. The schema describes a field
     * with the {@value #SCHEMA_NAME} as the {@link Schema#name() name} and {@link SchemaBuilder#string() STRING} for the literal
     * type storing the date in UTC ISO 8601 format (e.g., "2023-11-21").
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Converts a {@link java.time.LocalDateTime}, {@link LocalDate}, {@link LocalTime}, {@link java.util.Date},
     * {@link java.sql.Date}, {@link java.sql.Time}, or {@link java.sql.Timestamp} to its UTC ISO 8601 string representation
     * (e.g., "2023-11-21").
     * <p>
     * This method ignores time components of the input value. If an adjuster is provided, it's used to adjust the
     * {@link LocalDate} before converting it to the string representation.
     *
     * @param value the local or SQL date, time, or timestamp value; may not be null
     * @param adjuster the optional component that adjusts the local date value before obtaining the string representation;
     * may be null if no adjustment is necessary
     * @return the UTC ISO 8601 string representation of the date
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types
     */
    public static String toIsoString(Object value, TemporalAdjuster adjuster) {
        LocalDate date = Conversions.toLocalDate(value);
        if (adjuster != null) {
            date = date.with(adjuster);
        }
        ZonedDateTime zonedDate = ZonedDateTime.of(date, LocalTime.MIDNIGHT, ZoneOffset.UTC);
        return zonedDate.format(FORMATTER);
    }

    private IsoDate() {
    }
}