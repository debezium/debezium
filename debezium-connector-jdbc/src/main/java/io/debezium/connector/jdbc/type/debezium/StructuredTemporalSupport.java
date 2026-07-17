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
import java.util.OptionalInt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
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
                toNanoseconds(getPicoseconds(value)));
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
        final long picoseconds = getPicoseconds(value);
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);

        final StringBuilder builder = new StringBuilder(String.format("%02d:%02d:%02d", hour, minute, second));
        appendFraction(builder, picoseconds, 12);
        builder.append(formatOffset(offsetSeconds == null ? 0 : offsetSeconds));
        return builder.toString();
    }

    public static String toTimetzLiteral(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        requireFinite(value);
        final int hour = value.getInt8(StructuredTemporal.HOUR_FIELD);
        final int minute = value.getInt8(StructuredTemporal.MINUTE_FIELD);
        final int second = value.getInt8(StructuredTemporal.SECOND_FIELD);
        final long picoseconds = getPicoseconds(value);
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        final var fraction = StructuredTemporalPreflightValidator.reduceFraction(
                picoseconds, targetPrecision, handlingMode);

        final int totalSeconds = hour * 3_600 + minute * 60 + second + fraction.carrySeconds();
        if (totalSeconds < 0 || totalSeconds > 24 * 3_600) {
            throw new ConnectException("Structured zoned time is outside the PostgreSQL timetz range after precision reduction");
        }

        final int adjustedHour = totalSeconds / 3_600;
        final int adjustedMinute = totalSeconds % 3_600 / 60;
        final int adjustedSecond = totalSeconds % 60;
        final StringBuilder builder = new StringBuilder(String.format("%02d:%02d:%02d", adjustedHour, adjustedMinute, adjustedSecond));
        appendFraction(builder, fraction.picoseconds(), targetPrecision);
        builder.append(formatOffset(offsetSeconds == null ? 0 : offsetSeconds));
        return builder.toString();
    }

    public static void appendFraction(StringBuilder builder, long picoseconds, int precision) {
        if (picoseconds == 0 || precision == 0) {
            return;
        }
        final String digits = String.format("%012d", Math.abs(picoseconds));
        int length = precision;
        while (length > 0 && digits.charAt(length - 1) == '0') {
            --length;
        }
        if (length > 0) {
            builder.append('.').append(digits, 0, length);
        }
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

    public static LocalDateTime toLocalDateTime(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        final LocalDateTime dateTime = toLocalDateTimeWithoutFraction(value);
        final var fraction = StructuredTemporalPreflightValidator.reduceFraction(getPicoseconds(value), targetPrecision, handlingMode);
        return dateTime.withNano(fraction.nanoseconds()).plusSeconds(fraction.carrySeconds());
    }

    public static LocalTime toLocalTime(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        requireFinite(value);
        final LocalTime time = LocalTime.of(
                value.getInt8(StructuredTemporal.HOUR_FIELD),
                value.getInt8(StructuredTemporal.MINUTE_FIELD),
                value.getInt8(StructuredTemporal.SECOND_FIELD));
        final var fraction = StructuredTemporalPreflightValidator.reduceFraction(getPicoseconds(value), targetPrecision, handlingMode);
        return time.withNano(fraction.nanoseconds()).plusSeconds(fraction.carrySeconds());
    }

    public static OffsetDateTime toOffsetDateTime(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        final OffsetDateTime dateTime = OffsetDateTime.of(
                toLocalDateTimeWithoutFraction(value), offsetSeconds == null ? ZoneOffset.UTC : ZoneOffset.ofTotalSeconds(offsetSeconds));
        final var fraction = StructuredTemporalPreflightValidator.reduceFraction(getPicoseconds(value), targetPrecision, handlingMode);
        return dateTime.withNano(fraction.nanoseconds()).plusSeconds(fraction.carrySeconds());
    }

    public static String toLocalDateTimeLiteral(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        final LocalDateTime dateTime = toLocalDateTimeWithoutFraction(value);
        final var fraction = StructuredTemporalPreflightValidator.reduceFraction(getPicoseconds(value), targetPrecision, handlingMode);
        final LocalDateTime adjusted = dateTime.plusSeconds(fraction.carrySeconds());
        final StringBuilder builder = new StringBuilder(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                adjusted.getYear(), adjusted.getMonthValue(), adjusted.getDayOfMonth(), adjusted.getHour(), adjusted.getMinute(), adjusted.getSecond()));
        appendFraction(builder, fraction.picoseconds(), targetPrecision);
        return builder.toString();
    }

    public static long getPicoseconds(Struct value) {
        final Long picoseconds = value.getInt64(StructuredTemporal.PICOSECONDS_FIELD);
        if (picoseconds == null) {
            return 0;
        }
        if (picoseconds <= -StructuredTemporal.PICOSECONDS_PER_SECOND || picoseconds >= StructuredTemporal.PICOSECONDS_PER_SECOND) {
            throw new ConnectException("Structured temporal picoseconds must have an absolute value less than one second");
        }
        return picoseconds;
    }

    public static OptionalInt getPrecision(Schema schema) {
        if (schema != null && schema.parameters() != null) {
            final String precision = schema.parameters().get(StructuredTemporal.PRECISION_PARAMETER_KEY);
            if (precision != null) {
                return OptionalInt.of(Integer.parseInt(precision));
            }
            final String sourcePrecision = schema.parameters().get("__debezium.source.column.scale");
            if (sourcePrecision != null) {
                return OptionalInt.of(Integer.parseInt(sourcePrecision));
            }
            final String sourceLength = schema.parameters().get("__debezium.source.column.length");
            if (sourceLength != null) {
                return OptionalInt.of(Integer.parseInt(sourceLength));
            }
        }
        return OptionalInt.empty();
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
        final long picoseconds = getPicoseconds(value);
        final BigDecimal fractionalSeconds = BigDecimal.valueOf(seconds == null ? 0 : seconds)
                .add(BigDecimal.valueOf(picoseconds, 12))
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

    private static LocalDateTime toLocalDateTimeWithoutFraction(Struct value) {
        requireFinite(value);
        return LocalDateTime.of(
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD),
                value.getInt8(StructuredTemporal.HOUR_FIELD),
                value.getInt8(StructuredTemporal.MINUTE_FIELD),
                value.getInt8(StructuredTemporal.SECOND_FIELD));
    }

    private static int toNanoseconds(long picoseconds) {
        return Math.toIntExact(picoseconds / StructuredTemporal.PICOSECONDS_PER_NANOSECOND);
    }

    private StructuredTemporalSupport() {
    }
}
