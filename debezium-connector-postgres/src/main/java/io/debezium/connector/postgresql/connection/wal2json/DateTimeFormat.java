/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.wal2json;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.time.NanoTimestamp;

/**
 * Transformer for time/date related string representations in JSON messages coming from the wal2json plugin.
 *
 * @author Jiri Pechanec
 *
 */
public interface DateTimeFormat {
    public long timestamp(final String s);
    public long timestampWithTimeZone(final String s);
    public long systemTimestamp(final String s);
    public LocalDate date(final String s);
    public LocalTime time(final String s);
    public OffsetTime timeWithTimeZone(final String s);

    public static DateTimeFormat get() {
        return new ISODateTimeFormat();
    }
    public static class ISODateTimeFormat implements DateTimeFormat {
        private static final Logger LOGGER = LoggerFactory.getLogger(ISODateTimeFormat.class);

        private static final String TS_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
        private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern(TS_FORMAT_PATTERN);

        private static final String TS_TZ_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss[.S]X";
        private static final DateTimeFormatter TS_TZ_FORMAT = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .appendOffset("+HH", "")
                .toFormatter();

        private static final String SYSTEM_TS_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSSX";
        private static final DateTimeFormatter SYSTEM_TS_FORMAT = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .appendOffset("+HH", "Z")
                .toFormatter();

        private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";
        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern(DATE_FORMAT_PATTERN);

        private static final String TIME_FORMAT_PATTERN = "HH:mm:ss";
        private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern(TIME_FORMAT_PATTERN);

        private static final String TIME_TZ_FORMAT_PATTERN = "HH:mm:ssX";
        private static final DateTimeFormatter TIME_TZ_FORMAT = DateTimeFormatter.ofPattern(TIME_TZ_FORMAT_PATTERN);

        @Override
        public long timestamp(final String s) {
                return format(TS_FORMAT_PATTERN, s, () -> NanoTimestamp.toEpochNanos(LocalDateTime.parse(s, TS_FORMAT), null));
        }

        @Override
        public long timestampWithTimeZone(final String s) {
            return formatTZ(TS_TZ_FORMAT_PATTERN, TS_TZ_FORMAT, s);
        }

        @Override
        public LocalDate date(final String s) {
            return format(DATE_FORMAT_PATTERN, s, () -> LocalDate.parse(s, DATE_FORMAT));
        }

        @Override
        public LocalTime time(final String s) {
            return format(TIME_FORMAT_PATTERN, s, () -> LocalTime.parse(s, TIME_FORMAT));
        }

        @Override
        public OffsetTime timeWithTimeZone(final String s) {
            return format(TIME_TZ_FORMAT_PATTERN, s, () -> OffsetTime.parse(s, TIME_TZ_FORMAT)).withOffsetSameInstant(ZoneOffset.UTC);
        }

        @Override
        public long systemTimestamp(final String s) {
            return formatTZ(SYSTEM_TS_FORMAT_PATTERN, SYSTEM_TS_FORMAT, s);
        }

        private long formatTZ(final String pattern, final DateTimeFormatter formatter, final String s) {
            return format(pattern, s, () -> {
               final Instant ts = Instant.from(formatter.parse(s));
               return ts.getEpochSecond() * 1_000_000_000 + ts.getNano();
            });
        }

        private <T> T format(final String pattern, final String s, final Supplier<T> value) {
            try {
                return value.get();
            } catch (final DateTimeParseException e) {
                LOGGER.error("Cannot parse time/date value '{}', expected format '{}'", s, pattern);
                throw new ConnectException(e);
            }
        }
    }
}
