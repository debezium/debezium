/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * Temporal conversion constants.
 *
 * @author Randall Hauch
 */
public final class Conversions {

    static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    static final long MICROSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);
    static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    static final long NANOSECONDS_PER_MICROSECOND = TimeUnit.MICROSECONDS.toNanos(1);
    static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    static final long NANOSECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);
    static final long SECONDS_PER_DAY = TimeUnit.DAYS.toSeconds(1);
    static final long MICROSECONDS_PER_DAY = TimeUnit.DAYS.toMicros(1);
    static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private Conversions() {
    }

    @SuppressWarnings("deprecation")
    protected static LocalDate toLocalDate(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalDate) {
            return (LocalDate) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalDate();
        }
        if (obj instanceof java.sql.Date) {
            return ((java.sql.Date) obj).toLocalDate();
        }
        if (obj instanceof java.sql.Time) {
            throw new IllegalArgumentException("Unable to convert to LocalDate from a java.sql.Time value '" + obj + "'");
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            return LocalDate.of(date.getYear() + 1900,
                    date.getMonth() + 1,
                    date.getDate());
        }
        if (obj instanceof Long) {
            // Assume the value is the epoch day number
            return LocalDate.ofEpochDay((Long) obj);
        }
        if (obj instanceof Integer) {
            // Assume the value is the epoch day number
            return LocalDate.ofEpochDay((Integer) obj);
        }
        throw new IllegalArgumentException("Unable to convert to LocalDate from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    @SuppressWarnings("deprecation")
    protected static LocalTime toLocalTime(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LocalTime) {
            return (LocalTime) obj;
        }
        if (obj instanceof LocalDateTime) {
            return ((LocalDateTime) obj).toLocalTime();
        }
        if (obj instanceof java.sql.Date) {
            throw new IllegalArgumentException("Unable to convert to LocalDate from a java.sql.Date value '" + obj + "'");
        }
        if (obj instanceof java.sql.Time) {
            java.sql.Time time = (java.sql.Time) obj;
            long millis = (int) (time.getTime() % Conversions.MILLISECONDS_PER_SECOND);
            int nanosOfSecond = (int) (millis * Conversions.NANOSECONDS_PER_MILLISECOND);
            return LocalTime.of(time.getHours(),
                    time.getMinutes(),
                    time.getSeconds(),
                    nanosOfSecond);
        }
        if (obj instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
            return LocalTime.of(timestamp.getHours(),
                    timestamp.getMinutes(),
                    timestamp.getSeconds(),
                    timestamp.getNanos());
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            long millis = (int) (date.getTime() % Conversions.MILLISECONDS_PER_SECOND);
            int nanosOfSecond = (int) (millis * Conversions.NANOSECONDS_PER_MILLISECOND);
            return LocalTime.of(date.getHours(),
                    date.getMinutes(),
                    date.getSeconds(),
                    nanosOfSecond);
        }
        if (obj instanceof Duration) {
            Long value = ((Duration) obj).toNanos();
            if (value >= 0 && value <= NANOSECONDS_PER_DAY) {
                return LocalTime.ofNanoOfDay(value);
            }
            else {
                throw new IllegalArgumentException("Time values must use number of milliseconds greater than 0 and less than 86400000000000");
            }
        }
        throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    @SuppressWarnings("deprecation")
    protected static LocalDateTime toLocalDateTime(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof OffsetDateTime) {
            return ((OffsetDateTime) obj).toLocalDateTime();
        }
        if (obj instanceof Instant) {
            return ((Instant) obj).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        if (obj instanceof LocalDateTime) {
            return (LocalDateTime) obj;
        }
        if (obj instanceof LocalDate) {
            LocalDate date = (LocalDate) obj;
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }
        if (obj instanceof LocalTime) {
            LocalTime time = (LocalTime) obj;
            return LocalDateTime.of(EPOCH, time);
        }
        if (obj instanceof java.sql.Date) {
            java.sql.Date sqlDate = (java.sql.Date) obj;
            LocalDate date = sqlDate.toLocalDate();
            return LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }
        if (obj instanceof java.sql.Time) {
            LocalTime localTime = toLocalTime(obj);
            return LocalDateTime.of(EPOCH, localTime);
        }
        if (obj instanceof java.sql.Timestamp) {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
            return LocalDateTime.of(timestamp.getYear() + 1900,
                    timestamp.getMonth() + 1,
                    timestamp.getDate(),
                    timestamp.getHours(),
                    timestamp.getMinutes(),
                    timestamp.getSeconds(),
                    timestamp.getNanos());
        }
        if (obj instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) obj;
            long millis = (int) (date.getTime() % Conversions.MILLISECONDS_PER_SECOND);
            if (millis < 0) {
                millis = Conversions.MILLISECONDS_PER_SECOND + millis;
            }
            int nanosOfSecond = (int) (millis * Conversions.NANOSECONDS_PER_MILLISECOND);
            return LocalDateTime.of(date.getYear() + 1900,
                    date.getMonth() + 1,
                    date.getDate(),
                    date.getHours(),
                    date.getMinutes(),
                    date.getSeconds(),
                    nanosOfSecond);
        }
        throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" + obj + "' of type " + obj.getClass().getName());
    }

    public static long toEpochMicros(Instant instant) {
        return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
    }

    public static Instant toInstantFromMicros(long microsSinceEpoch) {
        return Instant.ofEpochSecond(
                TimeUnit.MICROSECONDS.toSeconds(microsSinceEpoch),
                TimeUnit.MICROSECONDS.toNanos(microsSinceEpoch % TimeUnit.SECONDS.toMicros(1)));
    }

    public static Instant toInstantFromMillis(long millisecondSinceEpoch) {
        return Instant.ofEpochSecond(
                TimeUnit.MILLISECONDS.toSeconds(millisecondSinceEpoch),
                TimeUnit.MILLISECONDS.toNanos(millisecondSinceEpoch % TimeUnit.SECONDS.toMillis(1)));
    }
}
