/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjuster;
import java.util.Locale;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A utility for converting various Java time representations into the {@link SchemaBuilder#string() STRING} representation of
 * the time and date in a particular time zone, and for defining a Kafka Connect {@link Schema} for zoned timestamp values.
 * <p>
 * The ISO date-time format includes the date, time (including fractional parts), and offset from UTC, such as
 * '2011-12-03T10:15:30+01:00'.
 *
 * @author Randall Hauch
 * @see Timestamp
 * @see MicroTimestamp
 * @see NanoTimestamp
 * @see ZonedTime
 */
public class ZonedTimestamp {

    /**
     * The ISO date-time format includes the date, time (including fractional parts), and offset from UTC, such as
     * '2011-12-03T10:15:30.030431+01:00'.
     */
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    /**
     * Returns a {@link DateTimeFormatter} that ensures that exactly fractionalWidth number of digits are present
     * in the nanosecond part of the datetime. If fractionWidth is null, then
     * {@link DateTimeFormatter.ISO_OFFSET_DATE_TIME} formatter is used, which can have anywhere from 0-9 digits in the
     * nanosecond part.
     *
     * @param fractionalWidth the optional component that specifies the exact number of digits to be present in a zoneddatetime
     *                        formatted string.
     * @return {@link DateTimeFormatter} containing exactly fractionalWidth number of digits in nanosecond part of the
     * datetime. If null, {@link DateTimeFormatter.ISO_OFFSET_DATE_TIME} formatter is used, which can have anywhere
     * from 0-9 digits in the nanosecond part.
     */
    private static DateTimeFormatter getDateTimeFormatter(Integer fractionalWidth) {
        // TIMESTAMP type passes fractionalWidth as -1.
        if (fractionalWidth == null || fractionalWidth <= 0 || fractionalWidth > 9) {
            return FORMATTER;
        }

        final DateTimeFormatter timeFormatter = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .optionalStart()
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendFraction(NANO_OF_SECOND, fractionalWidth, fractionalWidth, true)
                .toFormatter(Locale.ENGLISH);

        final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE)
                .appendLiteral('T')
                .append(timeFormatter)
                .toFormatter(Locale.ENGLISH);

        return new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(dateTimeFormatter)
                .parseLenient()
                .appendOffsetId()
                .parseStrict()
                .toFormatter(Locale.ENGLISH);
    }

    public static final String SCHEMA_NAME = "io.debezium.time.ZonedTimestamp";

    /**
     * Returns a {@link SchemaBuilder} for a {@link ZonedTimestamp}. You can use the resulting SchemaBuilder
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
     * Returns a Schema for a {@link ZonedTimestamp} but with all other default Schema settings.
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
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @param fractionalWidth the optional component that specifies the exact number of digits to be present in a zoneddatetime
     *                        formatted string.
     * @return the microseconds past midnight
     * @throws IllegalArgumentException if the value is not an instance of the acceptable types
     */
    public static String toIsoString(Object value, ZoneId defaultZone, TemporalAdjuster adjuster, Integer fractionalWidth) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof OffsetDateTime) {
            return toIsoString((OffsetDateTime) value, adjuster);
        }
        if (value instanceof ZonedDateTime) {
            return toIsoString((ZonedDateTime) value, adjuster, fractionalWidth);
        }
        if (value instanceof OffsetTime) {
            return toIsoString((OffsetTime) value, adjuster);
        }
        if (value instanceof java.util.Date) { // or JDBC subtypes
            return toIsoString((java.util.Date) value, defaultZone, adjuster);
        }
        throw new IllegalArgumentException(
                "Unable to convert to OffsetDateTime from unexpected value '" + value + "' of type " + value.getClass().getName());
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link OffsetDateTime}.
     *
     * @param timestamp the timestamp value
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(OffsetDateTime timestamp, TemporalAdjuster adjuster) {
        if (adjuster != null) {
            timestamp = timestamp.with(adjuster);
        }
        return timestamp.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link ZonedDateTime}.
     *
     * @param timestamp the timestamp value
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @param fractionalWidth the optional component that specifies the exact number of digits to be present in a zoneddatetime
     *                        formatted string.
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(ZonedDateTime timestamp, TemporalAdjuster adjuster, Integer fractionalWidth) {
        if (adjuster != null) {
            timestamp = timestamp.with(adjuster);
        }
        return timestamp.format(getDateTimeFormatter(fractionalWidth));
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link OffsetTime}.
     *
     * @param timestamp the timestamp value
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(OffsetTime timestamp, TemporalAdjuster adjuster) {
        if (adjuster != null) {
            timestamp = timestamp.with(adjuster);
        }
        return timestamp.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.util.Date} or one of its JDBC subclasses, using
     * the supplied timezone information.
     *
     * @param timestamp the timestamp value
     * @param zoneId the timezone identifier or offset where the timestamp is defined
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.util.Date timestamp, ZoneId zoneId, TemporalAdjuster adjuster) {
        if (timestamp instanceof java.sql.Timestamp) {
            return toIsoString((java.sql.Timestamp) timestamp, zoneId, adjuster);
        }
        if (timestamp instanceof java.sql.Date) {
            return toIsoString((java.sql.Date) timestamp, zoneId, adjuster);
        }
        if (timestamp instanceof java.sql.Time) {
            return toIsoString((java.sql.Time) timestamp, zoneId, adjuster);
        }
        return timestamp.toInstant().atZone(zoneId).format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.sql.Timestamp}, which contains a date and time but
     * has no timezone information.
     *
     * @param timestamp the JDBC timestamp value
     * @param zoneId the timezone identifier or offset where the timestamp is defined
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.sql.Timestamp timestamp, ZoneId zoneId, TemporalAdjuster adjuster) {
        Instant instant = timestamp.toInstant();
        if (adjuster != null) {
            instant = instant.with(adjuster);
        }
        ZonedDateTime zdt = instant.atZone(zoneId);
        return zdt.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.sql.Date}, which contains a date but no time or
     * timezone information.
     *
     * @param date the date value
     * @param zoneId the timezone identifier or offset where the date is defined
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.sql.Date date, ZoneId zoneId, TemporalAdjuster adjuster) {
        LocalDate localDate = date.toLocalDate();
        if (adjuster != null) {
            localDate = localDate.with(adjuster);
        }
        ZonedDateTime zdt = ZonedDateTime.of(localDate, LocalTime.MIDNIGHT, zoneId);
        return zdt.format(FORMATTER);
    }

    /**
     * Get the ISO 8601 formatted representation of the given {@link java.sql.Time}, which contains time but no date or timezone
     * information.
     *
     * @param time the JDBC time value
     * @param zoneId the timezone identifier or offset where the time is defined
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     * adjustment is necessary
     * @return the ISO 8601 formatted string
     */
    public static String toIsoString(java.sql.Time time, ZoneId zoneId, TemporalAdjuster adjuster) {
        LocalTime localTime = time.toLocalTime();
        if (adjuster != null) {
            localTime = localTime.with(adjuster);
        }
        ZonedDateTime zdt = ZonedDateTime.of(Conversions.EPOCH, localTime, zoneId);
        return zdt.format(FORMATTER);
    }

    private ZonedTimestamp() {
    }
}
