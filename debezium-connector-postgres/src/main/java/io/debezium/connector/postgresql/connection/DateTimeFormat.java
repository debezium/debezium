/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresValueConverter;

/**
 * Transformer for time/date related string representations in replication messages.
 *
 * @author Jiri Pechanec
 *
 */
public interface DateTimeFormat {
    Instant timestampToInstant(String s);

    OffsetDateTime timestampWithTimeZoneToOffsetDateTime(String s);

    Instant systemTimestampToInstant(String s);

    LocalDate date(String s);

    LocalTime time(String s);

    OffsetTime timeWithTimeZone(String s);

    static DateTimeFormat get() {
        return new ISODateTimeFormat();
    }

    class ISODateTimeFormat implements DateTimeFormat {
        private static final Logger LOGGER = LoggerFactory.getLogger(ISODateTimeFormat.class);

        // This formatter is similar to standard Java's ISO_LOCAL_DATE. But this one is
        // using 'YEAR_OF_ERA + SignStyle.NEVER' instead of 'YEAR+SignStyle.EXCEEDS_PAD'
        // to support ChronoField.ERA at the end of the date string.
        private static final DateTimeFormatter NON_ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR_OF_ERA, 4, 10, SignStyle.NEVER)
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();

        private static final String TS_FORMAT_PATTERN_HINT = "y..y-MM-dd HH:mm:ss[.S]";
        private static final DateTimeFormatter TS_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String TS_TZ_FORMAT_PATTERN_HINT = "y..y-MM-dd HH:mm:ss[.S]X";
        private static final DateTimeFormatter TS_TZ_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:mm", "")
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();
        private static final DateTimeFormatter TS_TZ_WITH_SECONDS_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:MM:SS", "")
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String SYSTEM_TS_FORMAT_PATTERN_HINT = "y..y-MM-dd HH:mm:ss.SSSSSSX";
        private static final DateTimeFormatter SYSTEM_TS_FORMAT = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:mm", "Z")
                .optionalStart()
                .appendLiteral(" ")
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String DATE_FORMAT_OPT_ERA_PATTERN_HINT = "y..y-MM-dd[ GG]";
        private static final DateTimeFormatter DATE_FORMAT_OPT_ERA = new DateTimeFormatterBuilder()
                .append(NON_ISO_LOCAL_DATE)
                .optionalStart()
                .appendLiteral(' ')
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter();

        private static final String TIME_FORMAT_PATTERN = "HH:mm:ss[.S]";
        private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .toFormatter();

        private static final String TIME_TZ_FORMAT_PATTERN = "HH:mm:ss[.S]X";
        private static final DateTimeFormatter TIME_TZ_FORMAT = new DateTimeFormatterBuilder()
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .appendOffset("+HH:mm", "")
                .toFormatter();

        @Override
        public LocalDate date(final String s) {
            return format(DATE_FORMAT_OPT_ERA_PATTERN_HINT, s, () -> {
                if ("infinity".equals(s)) {
                    return PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE;
                }
                else if ("-infinity".equals(s)) {
                    return PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE;
                }
                else {
                    return LocalDate.parse(s, DATE_FORMAT_OPT_ERA);
                }
            });
        }

        @Override
        public LocalTime time(final String s) {
            return format(TIME_FORMAT_PATTERN, s, () -> LocalTime.parse(s, TIME_FORMAT));
        }

        @Override
        public OffsetTime timeWithTimeZone(final String s) {
            return format(TIME_TZ_FORMAT_PATTERN, s, () -> OffsetTime.parse(s, TIME_TZ_FORMAT)).withOffsetSameInstant(ZoneOffset.UTC);
        }

        private <T> T format(final String pattern, final String s, final Supplier<T> value) {
            try {
                return value.get();
            }
            catch (final DateTimeParseException e) {
                LOGGER.error("Cannot parse time/date value '{}', expected format '{}'", s, pattern);
                throw new ConnectException(e);
            }
        }

        @Override
        public Instant timestampToInstant(String s) {
            return format(TS_FORMAT_PATTERN_HINT, s, () -> LocalDateTime.from(TS_FORMAT.parse(s)).toInstant(ZoneOffset.UTC));
        }

        @Override
        public OffsetDateTime timestampWithTimeZoneToOffsetDateTime(String s) {
            return format(TS_TZ_FORMAT_PATTERN_HINT, s, () -> {
                TemporalAccessor parsedTimestamp;
                // Usually the timestamp contains only hour offset and optionally minutes
                // For very large negative timestamps the offset could contain seconds
                // The standard parsing library does not allow both optional minutes and seconds in offset,
                // so it is necessary to parse it with optional minutes and if that fails then retyr with
                // seconds
                try {
                    parsedTimestamp = TS_TZ_FORMAT.parse(s);
                }
                catch (DateTimeParseException e) {
                    parsedTimestamp = TS_TZ_WITH_SECONDS_FORMAT.parse(s);
                }
                return OffsetDateTime.from(parsedTimestamp);
            });
        }

        @Override
        public Instant systemTimestampToInstant(String s) {
            return format(SYSTEM_TS_FORMAT_PATTERN_HINT, s, () -> OffsetDateTime.from(SYSTEM_TS_FORMAT.parse(s)).toInstant());
        }
    }
}
