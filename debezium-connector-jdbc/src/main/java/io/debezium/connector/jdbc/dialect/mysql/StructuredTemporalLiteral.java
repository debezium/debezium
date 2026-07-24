/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalRangeLossHandlingMode;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.connector.jdbc.type.debezium.TemporalRange;
import io.debezium.connector.jdbc.type.debezium.TemporalRange.Boundary;
import io.debezium.time.StructuredTemporal;

final class StructuredTemporalLiteral {

    static String date(Struct value) {
        return date(value, TemporalRange.unbounded(), TemporalRangeLossHandlingMode.FAIL, true, "target temporal type");
    }

    static String date(Struct value, TemporalRange range, TemporalRangeLossHandlingMode handlingMode,
                       boolean zeroDateSupported, String targetDescription) {
        if (zeroDateSupported && StructuredTemporal.isFinite(value) && hasZeroDate(value)) {
            return formatDate(
                    value.getInt32(StructuredTemporal.YEAR_FIELD),
                    value.getInt8(StructuredTemporal.MONTH_FIELD),
                    value.getInt8(StructuredTemporal.DAY_FIELD));
        }
        final Boundary adjusted = StructuredTemporalSupport.adjustDate(value, range, handlingMode, targetDescription);
        return formatDate(adjusted.year(), adjusted.month(), adjusted.day());
    }

    private static String formatDate(int year, int month, int day) {
        return String.format("%04d-%02d-%02d",
                year, month, day);
    }

    static String timestamp(Struct value, int precision, TemporalPrecisionLossHandlingMode handlingMode) {
        return timestamp(value, precision, handlingMode, TemporalRange.unbounded(),
                TemporalRangeLossHandlingMode.FAIL, true, "target temporal type");
    }

    static String timestamp(Struct value, int precision, TemporalPrecisionLossHandlingMode precisionHandlingMode,
                            TemporalRange range, TemporalRangeLossHandlingMode rangeHandlingMode,
                            boolean zeroDateSupported, String targetDescription) {
        if (zeroDateSupported && StructuredTemporal.isFinite(value) && hasZeroDate(value)) {
            final var fraction = StructuredTemporalPreflightValidator.reduceFraction(
                    StructuredTemporalSupport.getPicoseconds(value), precision, precisionHandlingMode);
            if (fraction.carrySeconds() == 0) {
                return formatTimestamp(
                        value.getInt32(StructuredTemporal.YEAR_FIELD),
                        value.getInt8(StructuredTemporal.MONTH_FIELD),
                        value.getInt8(StructuredTemporal.DAY_FIELD),
                        value.getInt8(StructuredTemporal.HOUR_FIELD),
                        value.getInt8(StructuredTemporal.MINUTE_FIELD),
                        value.getInt8(StructuredTemporal.SECOND_FIELD),
                        fraction.picoseconds(), precision);
            }
        }
        final Boundary adjusted = StructuredTemporalSupport.adjustTimestamp(
                value, precision, precisionHandlingMode, range, rangeHandlingMode, targetDescription);
        return formatTimestamp(adjusted.year(), adjusted.month(), adjusted.day(), adjusted.hour(), adjusted.minute(),
                adjusted.second(), adjusted.picoseconds(), precision);
    }

    static String duration(Struct value, int precision, TemporalPrecisionLossHandlingMode handlingMode) {
        requireZero(value, StructuredTemporal.YEARS_FIELD);
        requireZero(value, StructuredTemporal.MONTHS_FIELD);
        requireZero(value, StructuredTemporal.DAYS_FIELD);
        StructuredTemporalPreflightValidator.reduceFraction(
                longValue(value, StructuredTemporal.PICOSECONDS_FIELD), precision, handlingMode);

        BigDecimal totalSeconds = BigDecimal.valueOf(intValue(value, StructuredTemporal.HOURS_FIELD)).multiply(BigDecimal.valueOf(3_600))
                .add(BigDecimal.valueOf(intValue(value, StructuredTemporal.MINUTES_FIELD)).multiply(BigDecimal.valueOf(60)))
                .add(BigDecimal.valueOf(longValue(value, StructuredTemporal.SECONDS_FIELD)))
                .add(BigDecimal.valueOf(longValue(value, StructuredTemporal.PICOSECONDS_FIELD), 12));
        final RoundingMode roundingMode = handlingMode == TemporalPrecisionLossHandlingMode.ROUND ? RoundingMode.HALF_UP : RoundingMode.DOWN;
        totalSeconds = totalSeconds.setScale(precision, roundingMode);

        final BigDecimal maxSeconds = BigDecimal.valueOf(838L * 3_600 + 59L * 60 + 59L)
                .add(BigDecimal.ONE.subtract(BigDecimal.ONE.movePointLeft(precision)));
        if (totalSeconds.abs().compareTo(maxSeconds) > 0) {
            throw new ConnectException(String.format(
                    "Structured duration value '%s' is outside the MySQL TIME range",
                    StructuredTemporalSupport.toDurationString(value)));
        }

        final boolean negative = totalSeconds.signum() < 0;
        BigDecimal remaining = totalSeconds.abs();
        final int hours = remaining.divideToIntegralValue(BigDecimal.valueOf(3_600)).intValueExact();
        remaining = remaining.remainder(BigDecimal.valueOf(3_600));
        final int minutes = remaining.divideToIntegralValue(BigDecimal.valueOf(60)).intValueExact();
        remaining = remaining.remainder(BigDecimal.valueOf(60));
        final int seconds = remaining.intValue();
        final int fraction = remaining.remainder(BigDecimal.ONE).movePointRight(precision).intValue();

        final String suffix = precision == 0 ? "" : "." + String.format("%0" + precision + "d", fraction);
        return String.format("%s%03d:%02d:%02d%s", negative ? "-" : "", hours, minutes, seconds, suffix);
    }

    private static String formatTimestamp(int year, int month, int day, int hour, int minute, int second, long picoseconds, int precision) {
        final long fraction = precision == 0 ? 0 : picoseconds / (long) Math.pow(10, 12 - precision);
        final String suffix = precision == 0 ? "" : "." + String.format("%0" + precision + "d", fraction);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d%s", year, month, day, hour, minute, second, suffix);
    }

    private static void requireZero(Struct value, String fieldName) {
        if (intValue(value, fieldName) != 0) {
            throw new ConnectException(String.format(
                    "MySQL TIME cannot represent structured duration field '%s' without semantic loss", fieldName));
        }
    }

    private static boolean hasZeroDate(Struct value) {
        final Integer year = value.getInt32(StructuredTemporal.YEAR_FIELD);
        final Byte month = value.getInt8(StructuredTemporal.MONTH_FIELD);
        final Byte day = value.getInt8(StructuredTemporal.DAY_FIELD);
        return (year == null || year == 0)
                && (month == null || month == 0)
                && (day == null || day == 0);
    }

    private static int intValue(Struct value, String fieldName) {
        final Integer fieldValue = value.getInt32(fieldName);
        return fieldValue == null ? 0 : fieldValue;
    }

    private static long longValue(Struct value, String fieldName) {
        final Long fieldValue = value.getInt64(fieldName);
        return fieldValue == null ? 0 : fieldValue;
    }

    private static void requireFinite(Struct value) {
        if (!StructuredTemporal.isFinite(value)) {
            throw new ConnectException("MySQL does not support structured temporal infinity values");
        }
    }

    private StructuredTemporalLiteral() {
    }
}
