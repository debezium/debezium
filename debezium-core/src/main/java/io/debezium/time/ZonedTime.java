/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into the {@link SchemaBuilder#string() STRING} representation of
 * the time in a particular time zone, and for defining a Kafka Connect {@link Schema} for zoned time values.
 * <p>
 * The ISO date-time format includes the time (including fractional parts) and offset from UTC, such as
 * '10:15:30+01:00'.
 * 
 * @author Randall Hauch
 * @see Date
 * @see Time
 * @see Timestamp
 * @see ZonedTime
 * @see ZonedTimestamp
 */
public class ZonedTime {

    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_TIME;

    public static final String SCHEMA_NAME = "io.debezium.time.ZonedTime";

    /**
     * Returns a {@link SchemaBuilder} for a {@link ZonedTime}. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     * 
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                            .name(SCHEMA_NAME)
                            .version(1);
    }

    /**
     * Returns a Schema for a {@link ZonedTime} but with all other default Schema settings.
     * 
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.time.LocalDateTime}, {@link java.time.LocalDate},
     * {@link java.time.LocalTime}, {@link java.util.Date}, {@link java.sql.Date}, {@link java.sql.Time},
     * {@link java.sql.Timestamp}, {@link OffsetTime}, or {@link OffsetDateTime}, ignoring any date portions of the supplied
     * value.
     * 
     * @param value the local or SQL date, time, or timestamp value; may not be null
     * @param defaultZone the time zone that should be used by default if the value does not have timezone information; may not be
     *            null
     * @return the microseconds past midnight
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types or is null
     */
    public static String toIsoString(Object value, ZoneId defaultZone) {
        if ( value instanceof OffsetTime ) {
            return toIsoString((OffsetTime) value);
        }
        if ( value instanceof OffsetDateTime ) {
            return toIsoString((OffsetDateTime) value);
        }
        if (value instanceof java.util.Date) {  // or JDBC subtypes
            return toIsoString((java.util.Date) value,defaultZone);
        }
        throw new IllegalArgumentException("Unable to convert to OffsetTime from unexpected value '" + value + "' of type " + value.getClass().getName());
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link OffsetDateTime}.
     * 
     * @param timestamp the timestamp value; may not be null
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(OffsetDateTime timestamp) {
        return timestamp.toOffsetTime().format(FORMATTER);
    }
    /**
     * Get the ISO 8601 formatted representation of the given {@link OffsetTime}.
     * 
     * @param timestamp the timestamp value; may not be null
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(OffsetTime timestamp) {
        return timestamp.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.util.Date} or one of its JDBC subclasses, using
     * the supplied timezone information.
     * 
     * @param timestamp the timestamp value; may not be null
     * @param zoneId the timezone identifier or offset where the timestamp is defined
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.util.Date timestamp, ZoneId zoneId) {
        if (timestamp instanceof java.sql.Timestamp) {
            return toIsoString((java.sql.Timestamp) timestamp, zoneId);
        }
        if (timestamp instanceof java.sql.Date) {
            return toIsoString((java.sql.Date) timestamp, zoneId);
        }
        if (timestamp instanceof java.sql.Time) {
            return toIsoString((java.sql.Time) timestamp, zoneId);
        }
        return timestamp.toInstant().atZone(zoneId).format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.sql.Timestamp}, which contains a date and time but
     * has no timezone information.
     * 
     * @param timestamp the JDBC timestamp value; may not be null
     * @param zoneId the timezone identifier or offset where the timestamp is defined
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.sql.Timestamp timestamp, ZoneId zoneId) {
        ZonedDateTime zdt = timestamp.toInstant().atZone(zoneId);
        return zdt.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.sql.Date}, which contains a date but no time or
     * timezone information.
     * 
     * @param date the date value; may not be null
     * @param zoneId the timezone identifier or offset where the date is defined
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.sql.Date date, ZoneId zoneId) {
        LocalDate localDate = date.toLocalDate();
        ZonedDateTime zdt = ZonedDateTime.of(localDate, LocalTime.MIDNIGHT, zoneId);
        return zdt.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.sql.Time}, which contains time but no date or timezone
     * information.
     * 
     * @param time the JDBC time value; may not be null
     * @param zoneId the timezone identifier or offset where the time is defined
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.sql.Time time, ZoneId zoneId) {
        LocalTime localTime = time.toLocalTime();
        ZonedDateTime zdt = ZonedDateTime.of(Conversions.EPOCH, localTime, zoneId);
        return zdt.format(FORMATTER);
    }

    private ZonedTime() {
    }
}
