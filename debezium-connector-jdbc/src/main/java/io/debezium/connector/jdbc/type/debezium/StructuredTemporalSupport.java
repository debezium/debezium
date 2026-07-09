/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.time.StructuredTemporal;

public final class StructuredTemporalSupport {

    public static LocalDate toLocalDate(Struct value) {
        requireFinite(value);
        return LocalDate.of(
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD));
    }

    public static LocalTime toLocalTime(Struct value) {
        requireFinite(value);
        return LocalTime.of(
                value.getInt8(StructuredTemporal.HOUR_FIELD),
                value.getInt8(StructuredTemporal.MINUTE_FIELD),
                value.getInt8(StructuredTemporal.SECOND_FIELD),
                value.getInt32(StructuredTemporal.NANOS_FIELD));
    }

    /**
     * Formats a {@code StructuredZonedTime} struct into a PostgreSQL {@code timetz} literal directly from its
     * raw components, so the original offset and the end-of-day boundary hour {@code 24} are preserved. This
     * intentionally avoids {@link LocalTime}/{@link java.time.OffsetTime}, which cannot represent hour 24.
     */
    public static String toTimetzLiteral(Struct value) {
        requireFinite(value);
        final int hour = value.getInt8(StructuredTemporal.HOUR_FIELD);
        final int minute = value.getInt8(StructuredTemporal.MINUTE_FIELD);
        final int second = value.getInt8(StructuredTemporal.SECOND_FIELD);
        final Integer nanos = value.getInt32(StructuredTemporal.NANOS_FIELD);
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);

        final StringBuilder builder = new StringBuilder(String.format("%02d:%02d:%02d", hour, minute, second));
        if (nanos != null && nanos > 0) {
            // Match java.time fractional-second formatting: emit 3, 6 or 9 digits, trimming trailing zero-triples.
            final String digits = String.format("%09d", nanos);
            int length = 9;
            while (length > 3 && digits.charAt(length - 1) == '0' && digits.charAt(length - 2) == '0'
                    && digits.charAt(length - 3) == '0') {
                length -= 3;
            }
            builder.append('.').append(digits, 0, length);
        }
        builder.append(formatOffset(offsetSeconds == null ? 0 : offsetSeconds));
        return builder.toString();
    }

    private static String formatOffset(int offsetSeconds) {
        final char sign = offsetSeconds < 0 ? '-' : '+';
        final int abs = Math.abs(offsetSeconds);
        final StringBuilder builder = new StringBuilder()
                .append(sign)
                .append(String.format("%02d:%02d", abs / 3600, (abs % 3600) / 60));
        final int seconds = abs % 60;
        if (seconds != 0) {
            builder.append(String.format(":%02d", seconds));
        }
        return builder.toString();
    }

    public static LocalDateTime toLocalDateTime(Struct value) {
        return LocalDateTime.of(toLocalDate(value), toLocalTime(value));
    }

    public static OffsetDateTime toOffsetDateTime(Struct value) {
        final LocalDateTime localDateTime = toLocalDateTime(value);
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        return OffsetDateTime.of(localDateTime, offsetSeconds == null ? ZoneOffset.UTC : ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    public static void requireFinite(Struct value) {
        if (!StructuredTemporal.isFinite(value)) {
            throw new ConnectException("Non-finite structured temporal values require dialect-specific handling");
        }
    }

    public static String toDurationString(Struct value) {
        final StringBuilder builder = new StringBuilder();
        append(builder, value.getInt32(StructuredTemporal.YEARS_FIELD), " years");
        append(builder, value.getInt32(StructuredTemporal.MONTHS_FIELD), " months");
        append(builder, value.getInt32(StructuredTemporal.DAYS_FIELD), " days");
        append(builder, value.getInt32(StructuredTemporal.HOURS_FIELD), " hours");
        append(builder, value.getInt32(StructuredTemporal.MINUTES_FIELD), " minutes");

        final Long seconds = value.getInt64(StructuredTemporal.SECONDS_FIELD);
        final Integer nanos = value.getInt32(StructuredTemporal.NANOS_FIELD);
        final BigDecimal fractionalSeconds = BigDecimal.valueOf(seconds == null ? 0 : seconds)
                .add(BigDecimal.valueOf(nanos == null ? 0 : nanos, 9))
                .stripTrailingZeros();
        if (fractionalSeconds.signum() != 0 || builder.length() == 0) {
            append(builder, fractionalSeconds.toPlainString(), " seconds");
        }
        return builder.toString().trim();
    }

    private static void append(StringBuilder builder, Integer value, String suffix) {
        if (value != null && value != 0) {
            append(builder, value.toString(), suffix);
        }
    }

    private static void append(StringBuilder builder, String value, String suffix) {
        if (builder.length() > 0) {
            builder.append(' ');
        }
        builder.append(value).append(suffix);
    }

    private StructuredTemporalSupport() {
    }
}
