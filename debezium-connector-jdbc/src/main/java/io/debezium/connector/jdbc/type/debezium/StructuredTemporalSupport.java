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
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalRangeLossHandlingMode;
import io.debezium.connector.jdbc.type.debezium.TemporalRange.Boundary;
import io.debezium.time.StructuredTemporal;

public final class StructuredTemporalSupport {

    public static LocalDate toLocalDate(Struct value) {
        requireFinite(value);
        return LocalDate.of(
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD));
    }

    public static LocalDate toLocalDate(Struct value, TemporalRange range, TemporalRangeLossHandlingMode handlingMode,
                                        String targetDescription) {
        return adjustDate(value, range, handlingMode, targetDescription).toLocalDate();
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
        return adjustTimestamp(value, targetPrecision, handlingMode, TemporalRange.unbounded(),
                TemporalRangeLossHandlingMode.FAIL, "target dialect").toLocalDateTime();
    }

    public static LocalDateTime toLocalDateTime(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode precisionHandlingMode,
                                                TemporalRange range, TemporalRangeLossHandlingMode rangeHandlingMode,
                                                String targetDescription) {
        return adjustTimestamp(value, targetPrecision, precisionHandlingMode, range, rangeHandlingMode,
                targetDescription).toLocalDateTime();
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
        return toOffsetDateTime(value, targetPrecision, handlingMode, TemporalRange.unbounded(),
                TemporalRangeLossHandlingMode.FAIL, "target dialect");
    }

    public static OffsetDateTime toOffsetDateTime(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode precisionHandlingMode,
                                                  TemporalRange range, TemporalRangeLossHandlingMode rangeHandlingMode,
                                                  String targetDescription) {
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        final Boundary adjusted = adjustTimestamp(value, targetPrecision, precisionHandlingMode, range, rangeHandlingMode,
                targetDescription);
        return OffsetDateTime.of(adjusted.toLocalDateTime(),
                offsetSeconds == null ? ZoneOffset.UTC : ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    public static String toLocalDateTimeLiteral(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        return toLocalDateTimeLiteral(value, targetPrecision, handlingMode, TemporalRange.unbounded(),
                TemporalRangeLossHandlingMode.FAIL, "target dialect");
    }

    public static String toLocalDateTimeLiteral(Struct value, int targetPrecision, TemporalPrecisionLossHandlingMode precisionHandlingMode,
                                                TemporalRange range, TemporalRangeLossHandlingMode rangeHandlingMode,
                                                String targetDescription) {
        final Boundary adjusted = adjustTimestamp(value, targetPrecision, precisionHandlingMode, range, rangeHandlingMode,
                targetDescription);
        final StringBuilder builder = new StringBuilder(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                adjusted.year(), adjusted.month(), adjusted.day(), adjusted.hour(), adjusted.minute(), adjusted.second()));
        appendFraction(builder, adjusted.picoseconds(), targetPrecision);
        return builder.toString();
    }

    public static Boundary adjustDate(Struct value, TemporalRange range, TemporalRangeLossHandlingMode handlingMode,
                                      String targetDescription) {
        if (!StructuredTemporal.isFinite(value)) {
            return saturateSpecialValue(value, range, handlingMode, 0, targetDescription);
        }
        final Boundary date = Boundary.date(
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD));
        return applyRange(date, range, handlingMode, 0, targetDescription);
    }

    public static Boundary adjustTimestamp(Struct value, int targetPrecision,
                                           TemporalPrecisionLossHandlingMode precisionHandlingMode,
                                           TemporalRange range, TemporalRangeLossHandlingMode rangeHandlingMode,
                                           String targetDescription) {
        if (!StructuredTemporal.isFinite(value)) {
            return saturateSpecialValue(value, range, rangeHandlingMode, targetPrecision, targetDescription);
        }

        final var fraction = StructuredTemporalPreflightValidator.reduceFraction(
                getPicoseconds(value), targetPrecision, precisionHandlingMode);
        if (fraction.picoseconds() < 0) {
            throw new ConnectException("Structured timestamp picoseconds must not be negative");
        }
        Boundary timestamp = Boundary.timestamp(
                value.getInt32(StructuredTemporal.YEAR_FIELD),
                value.getInt8(StructuredTemporal.MONTH_FIELD),
                value.getInt8(StructuredTemporal.DAY_FIELD),
                value.getInt8(StructuredTemporal.HOUR_FIELD),
                value.getInt8(StructuredTemporal.MINUTE_FIELD),
                value.getInt8(StructuredTemporal.SECOND_FIELD),
                fraction.picoseconds());
        timestamp = timestamp.plusSeconds(fraction.carrySeconds());
        return applyRange(timestamp, range, rangeHandlingMode, targetPrecision, targetDescription);
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

    private static Boundary applyRange(Boundary value, TemporalRange range, TemporalRangeLossHandlingMode handlingMode,
                                       int targetPrecision, String targetDescription) {
        if (range.contains(value)) {
            return value;
        }
        if (handlingMode == TemporalRangeLossHandlingMode.SATURATE) {
            return range.saturate(value).withPrecision(targetPrecision);
        }
        throw rangeException(value, range, targetDescription);
    }

    private static Boundary saturateSpecialValue(Struct value, TemporalRange range,
                                                 TemporalRangeLossHandlingMode handlingMode, int targetPrecision,
                                                 String targetDescription) {
        if (handlingMode == TemporalRangeLossHandlingMode.SATURATE) {
            if (StructuredTemporal.isPositiveInfinity(value) && range.maximum() != null) {
                return range.maximum().withPrecision(targetPrecision);
            }
            if (StructuredTemporal.isNegativeInfinity(value) && range.minimum() != null) {
                return range.minimum().withPrecision(targetPrecision);
            }
        }
        throw new ConnectException(String.format(
                "Non-finite structured temporal value cannot be represented by %s", targetDescription));
    }

    private static ConnectException rangeException(Boundary value, TemporalRange range, String targetDescription) {
        final String minimum = range.minimum() == null ? "unbounded" : range.minimum().toString();
        final String maximum = range.maximum() == null ? "unbounded" : range.maximum().toString();
        return new ConnectException(String.format(
                "Structured temporal value '%s' is outside the range [%s, %s] supported by %s; "
                        + "set 'temporal.range.loss.handling.mode' to 'saturate' to map it to the nearest boundary",
                value, minimum, maximum, targetDescription));
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
