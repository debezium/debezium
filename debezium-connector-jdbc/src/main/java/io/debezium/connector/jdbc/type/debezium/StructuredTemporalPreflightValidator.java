/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;

/**
 * Validates structured temporal values against the actual target column before JDBC statement execution.
 */
public final class StructuredTemporalPreflightValidator {

    public static void validatePrecision(ColumnDescriptor column, Struct value, int targetPrecision,
                                         TemporalPrecisionLossHandlingMode handlingMode) {
        if (value == null || !StructuredTemporal.isFinite(value)) {
            return;
        }

        final Long picoseconds = StructuredTemporalSupport.getPicoseconds(value);
        if (handlingMode == TemporalPrecisionLossHandlingMode.FAIL && hasDiscardedDigits(picoseconds, targetPrecision)) {
            throw new ConnectException(String.format(
                    "Structured temporal value for target column '%s' has non-zero fractional digits beyond precision %d; "
                            + "set 'temporal.precision.loss.handling.mode' to 'truncate' or 'round' to allow an explicit reduction",
                    column.getColumnName(), targetPrecision));
        }
    }

    public static void validateDuration(Schema schema, Struct value, TargetTemporalCapabilities capabilities) {
        final StructuredDuration.Kind kind = durationKind(schema, value);
        validateDurationComponents(kind, value);
        if (!capabilities.durationKinds().contains(kind)) {
            throw new ConnectException(String.format(
                    "Structured duration kind '%s' cannot be represented by the target dialect without semantic loss",
                    kind.getValue()));
        }
    }

    public static void validateZonedTimestamp(Struct value, TargetTemporalCapabilities capabilities) {
        if (value == null || !StructuredTemporal.isFinite(value)) {
            return;
        }

        final String zoneId = value.getString(StructuredTemporal.ZONE_ID_FIELD);
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        if (isRegionId(zoneId) && !capabilities.zonedTimestampSupport().preservesRegion()) {
            throw new ConnectException(String.format(
                    "Structured zoned timestamp region '%s' cannot be preserved by the target dialect", zoneId));
        }
        if (offsetSeconds != null && offsetSeconds != 0 && !capabilities.zonedTimestampSupport().preservesOffset()) {
            throw new ConnectException(String.format(
                    "Structured zoned timestamp offset %d cannot be preserved by the target dialect", offsetSeconds));
        }
    }

    public static boolean hasDiscardedDigits(Long picoseconds, int targetPrecision) {
        if (picoseconds == null || targetPrecision >= 12) {
            return false;
        }
        final long factor = precisionFactor(targetPrecision);
        return Math.abs(picoseconds) % factor != 0;
    }

    public static StructuredDuration.Kind durationKind(Schema schema, Struct value) {
        if (schema != null && schema.parameters() != null) {
            final String kindValue = schema.parameters().get(StructuredTemporal.DURATION_KIND_PARAMETER_KEY);
            if (kindValue != null) {
                final StructuredDuration.Kind kind = StructuredDuration.Kind.parse(kindValue);
                if (kind == null) {
                    throw new ConnectException("Unknown structured duration kind '" + kindValue + "'");
                }
                return kind;
            }
        }

        final boolean hasYearMonth = value != null
                && (nonZero(value.getInt32(StructuredTemporal.YEARS_FIELD)) || nonZero(value.getInt32(StructuredTemporal.MONTHS_FIELD)));
        final boolean hasDayTime = value != null
                && (nonZero(value.getInt32(StructuredTemporal.DAYS_FIELD))
                        || nonZero(value.getInt32(StructuredTemporal.HOURS_FIELD))
                        || nonZero(value.getInt32(StructuredTemporal.MINUTES_FIELD))
                        || nonZero(value.getInt64(StructuredTemporal.SECONDS_FIELD))
                        || StructuredTemporalSupport.getPicoseconds(value) != 0);
        if (hasYearMonth && hasDayTime) {
            return StructuredDuration.Kind.MIXED;
        }
        if (hasYearMonth) {
            return StructuredDuration.Kind.YEAR_MONTH;
        }
        if (value != null && nonZero(value.getInt32(StructuredTemporal.DAYS_FIELD))) {
            return StructuredDuration.Kind.DAY_TIME;
        }
        return StructuredDuration.Kind.ELAPSED_TIME;
    }

    public static ReducedFraction reduceFraction(long picoseconds, int targetPrecision, TemporalPrecisionLossHandlingMode handlingMode) {
        if (targetPrecision >= 12) {
            return new ReducedFraction(picoseconds, 0);
        }

        final long factor = precisionFactor(targetPrecision);
        if (handlingMode == TemporalPrecisionLossHandlingMode.FAIL && Math.abs(picoseconds) % factor != 0) {
            throw new ConnectException(String.format(
                    "Structured temporal value has non-zero fractional digits beyond precision %d", targetPrecision));
        }
        if (handlingMode == TemporalPrecisionLossHandlingMode.ROUND) {
            final long adjustment = picoseconds < 0 ? -factor / 2L : factor / 2L;
            final long rounded = (picoseconds + adjustment) / factor * factor;
            if (Math.abs(rounded) == StructuredTemporal.PICOSECONDS_PER_SECOND) {
                return new ReducedFraction(0, rounded < 0 ? -1 : 1);
            }
            return new ReducedFraction(rounded, 0);
        }
        return new ReducedFraction(picoseconds / factor * factor, 0);
    }

    private static long precisionFactor(int targetPrecision) {
        if (targetPrecision < 0 || targetPrecision > 12) {
            throw new ConnectException("Target temporal precision must be between 0 and 12, but was " + targetPrecision);
        }
        long factor = 1;
        for (int i = targetPrecision; i < 12; ++i) {
            factor *= 10;
        }
        return factor;
    }

    private static boolean nonZero(Number value) {
        return value != null && value.longValue() != 0;
    }

    private static void validateDurationComponents(StructuredDuration.Kind kind, Struct value) {
        if (value == null) {
            return;
        }
        final boolean hasYearMonth = nonZero(value.getInt32(StructuredTemporal.YEARS_FIELD))
                || nonZero(value.getInt32(StructuredTemporal.MONTHS_FIELD));
        final boolean hasDays = nonZero(value.getInt32(StructuredTemporal.DAYS_FIELD));
        final boolean hasTime = nonZero(value.getInt32(StructuredTemporal.HOURS_FIELD))
                || nonZero(value.getInt32(StructuredTemporal.MINUTES_FIELD))
                || nonZero(value.getInt64(StructuredTemporal.SECONDS_FIELD))
                || StructuredTemporalSupport.getPicoseconds(value) != 0;
        final boolean invalid = switch (kind) {
            case ELAPSED_TIME -> hasYearMonth || hasDays;
            case YEAR_MONTH -> hasDays || hasTime;
            case DAY_TIME -> hasYearMonth;
            case MIXED -> false;
        };
        if (invalid) {
            throw new ConnectException(String.format(
                    "Structured duration components do not match schema kind '%s'", kind.getValue()));
        }
    }

    private static boolean isRegionId(String zoneId) {
        if (zoneId == null) {
            return false;
        }
        try {
            return !(ZoneId.of(zoneId) instanceof ZoneOffset);
        }
        catch (DateTimeException e) {
            throw new ConnectException("Invalid structured zoned timestamp zone identifier '" + zoneId + "'", e);
        }
    }

    public record ReducedFraction(long picoseconds, int carrySeconds) {

        public int nanoseconds() {
            return Math.toIntExact(picoseconds / StructuredTemporal.PICOSECONDS_PER_NANOSECOND);
        }
    }

    private StructuredTemporalPreflightValidator() {
    }
}
