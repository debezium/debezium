/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.debezium.time.Conversions;

/**
 * Helper class for conversions for Temporal data type values.
 *
 * @author Chris Cranford
 */
public class DateTimeUtils {

    private DateTimeUtils() {
    }

    public static Instant toInstantFromNanos(long epochNanos) {
        final long epochSeconds = TimeUnit.NANOSECONDS.toSeconds(epochNanos);
        final long adjustment = TimeUnit.NANOSECONDS.toNanos(epochNanos % TimeUnit.SECONDS.toNanos(1));
        return Instant.ofEpochSecond(epochSeconds, adjustment);
    }

    public static ZonedDateTime toZonedDateTimeFromDate(Date date, TimeZone timeZone) {
        return toZonedDateTimeFromDate(date, timeZone.toZoneId());
    }

    public static ZonedDateTime toZonedDateTimeFromDate(Date date, ZoneId zoneId) {
        return date.toInstant().atZone(zoneId);
    }

    public static ZonedDateTime toZonedDateTimeFromInstantEpochMicros(long epochMicros) {
        return Conversions.toInstantFromMicros(epochMicros).atZone(ZoneOffset.UTC);
    }

    public static ZonedDateTime toZonedDateTimeFromInstantEpochNanos(long epochNanos) {
        return ZonedDateTime.ofInstant(toInstantFromNanos(epochNanos), ZoneOffset.UTC);
    }

    public static LocalDate toLocalDateOfEpochDays(long epochDays) {
        return LocalDate.ofEpochDay(epochDays);
    }

    public static LocalDate toLocalDateFromDate(Date date) {
        return toLocalDateFromInstantEpochMillis(date.getTime());
    }

    public static LocalDate toLocalDateFromInstantEpochMillis(long epochMillis) {
        return LocalDate.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    public static LocalTime toLocalTimeFromDurationMilliseconds(long durationMillis) {
        return LocalTime.ofNanoOfDay(Duration.of(durationMillis, ChronoUnit.MILLIS).toNanos());
    }

    public static LocalTime toLocalTimeFromDurationMicroseconds(long durationMicros) {
        return LocalTime.ofNanoOfDay(Duration.of(durationMicros, ChronoUnit.MICROS).toNanos());
    }

    public static LocalTime toLocalTimeFromDurationNanoseconds(long durationNanos) {
        return LocalTime.ofNanoOfDay(Duration.of(durationNanos, ChronoUnit.NANOS).toNanos());
    }

    public static LocalTime toLocalTimeFromUtcDate(Date date) {
        return date.toInstant().atOffset(ZoneOffset.UTC).toLocalTime();
    }

    public static LocalDateTime toLocalDateTimeFromDate(Date date) {
        return toLocalDateTimeFromInstantEpochMillis(date.getTime());
    }

    public static LocalDateTime toLocalDateTimeFromInstantEpochMillis(long epochMillis) {
        return LocalDateTime.ofInstant(Conversions.toInstantFromMillis(epochMillis), ZoneOffset.UTC);
    }

    public static LocalDateTime toLocalDateTimeFromInstantEpochMicros(long epochMicros) {
        return LocalDateTime.ofInstant(Conversions.toInstantFromMicros(epochMicros), ZoneOffset.UTC);
    }

    public static LocalDateTime toLocalDateTimeFromInstantEpochNanos(long epochNanos) {
        return LocalDateTime.ofInstant(toInstantFromNanos(epochNanos), ZoneOffset.UTC);
    }

    public static Timestamp toTimestampFromMillis(long epochMilliseconds) {
        final Instant instant = Conversions.toInstantFromMillis(epochMilliseconds);
        final Timestamp ts = new Timestamp(instant.toEpochMilli());
        ts.setNanos(instant.getNano());
        return ts;
    }

}
