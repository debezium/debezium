/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.type.AbstractTemporalType;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;

/**
 * Oracle implementation of {@link StructuredDuration} values.
 */
public class StructuredDurationType extends AbstractTemporalType {

    public static final StructuredDurationType INSTANCE = new StructuredDurationType();

    private enum IntervalKind {
        YEAR_MONTH,
        DAY_SECOND,
        TEXT
    }

    @Override
    public String[] getRegistrationKeys() {
        return StructuredDuration.schemaNames();
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return switch (resolveIntervalKind(schema)) {
            case YEAR_MONTH -> "TO_YMINTERVAL(?)";
            case DAY_SECOND -> "TO_DSINTERVAL(?)";
            case TEXT -> "?";
        };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return switch (resolveIntervalKind(schema)) {
            case YEAR_MONTH -> "INTERVAL YEAR TO MONTH";
            case DAY_SECOND -> getDaySecondTypeName(schema);
            case TEXT -> getDialect().getJdbcTypeName(Types.VARCHAR, Size.length(128));
        };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        final Struct struct = requireStruct(value);
        return switch (resolveIntervalKind(schema)) {
            case YEAR_MONTH -> "TO_YMINTERVAL('" + toYearMonthLiteral(struct) + "')";
            case DAY_SECOND -> "TO_DSINTERVAL('" + toDaySecondLiteral(
                    struct, getDaySecondPrecision(schema), getPrecisionLossHandlingMode()) + "')";
            case TEXT -> "'" + StructuredTemporalSupport.toDurationString(struct) + "'";
        };
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, toBindingValue(schema, requireStruct(value)), Types.VARCHAR));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return;
        }
        final Struct struct = requireStruct(value);
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        StructuredTemporalPreflightValidator.validateDuration(schema, struct, capabilities);
        if (resolveIntervalKind(schema) == IntervalKind.DAY_SECOND) {
            StructuredTemporalPreflightValidator.validatePrecision(
                    column, struct, capabilities.targetTimePrecision(column), getPrecisionLossHandlingMode());
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final Struct struct = requireStruct(value);
        if (resolveIntervalKind(schema) == IntervalKind.DAY_SECOND) {
            final int precision = getDialect().getTargetTemporalCapabilities().targetTimePrecision(column);
            return List.of(new ValueBindDescriptor(index,
                    toDaySecondLiteral(struct, precision, getPrecisionLossHandlingMode()), Types.VARCHAR));
        }
        return List.of(new ValueBindDescriptor(index, toBindingValue(schema, struct), Types.VARCHAR));
    }

    private String getDaySecondTypeName(Schema schema) {
        final String dayPrecision = getSourceColumnSize(schema)
                .filter(StructuredDurationType::isPositive)
                .map(value -> "(" + value + ")")
                .orElse("");
        return "INTERVAL DAY" + dayPrecision + " TO SECOND(" + getDaySecondPrecision(schema) + ")";
    }

    private String toBindingValue(Schema schema, Struct value) {
        return switch (resolveIntervalKind(schema)) {
            case YEAR_MONTH -> toYearMonthLiteral(value);
            case DAY_SECOND -> toDaySecondLiteral(value, getDaySecondPrecision(schema), getPrecisionLossHandlingMode());
            case TEXT -> StructuredTemporalSupport.toDurationString(value);
        };
    }

    private int getDaySecondPrecision(Schema schema) {
        return Math.min(StructuredTemporalSupport.getPrecision(schema).orElse(9), 9);
    }

    private IntervalKind resolveIntervalKind(Schema schema) {
        if (schema != null) {
            // Without a declared kind, for example from an earlier version, the propagated source type decides
            return StructuredTemporalPreflightValidator.declaredDurationKind(schema)
                    .map(kind -> switch (kind) {
                        case YEAR_MONTH -> IntervalKind.YEAR_MONTH;
                        case DAY_TIME -> IntervalKind.DAY_SECOND;
                        case MONTH_DAY_TIME -> IntervalKind.TEXT;
                    })
                    .orElseGet(() -> resolveIntervalKind(getSourceColumnType(schema).orElse(null)));
        }
        return IntervalKind.TEXT;
    }

    private IntervalKind resolveIntervalKind(String typeName) {
        if (typeName == null) {
            return IntervalKind.TEXT;
        }

        final String upperTypeName = typeName.toUpperCase(Locale.ENGLISH);
        if (upperTypeName.contains("INTERVALYM") || (upperTypeName.contains("YEAR") && upperTypeName.contains("MONTH"))) {
            return IntervalKind.YEAR_MONTH;
        }
        if (upperTypeName.contains("INTERVALDS") || (upperTypeName.contains("DAY") && upperTypeName.contains("SECOND"))) {
            return IntervalKind.DAY_SECOND;
        }
        return IntervalKind.TEXT;
    }

    private String toYearMonthLiteral(Struct value) {
        // A year-month duration is a single signed number of months, even if its components have different signs
        final long totalMonths = intValue(value, StructuredTemporal.YEARS_FIELD) * 12L + intValue(value, StructuredTemporal.MONTHS_FIELD);
        final long absoluteMonths = Math.abs(totalMonths);
        return String.format("%s%d-%02d", totalMonths < 0 ? "-" : "", absoluteMonths / 12, absoluteMonths % 12);
    }

    private String toDaySecondLiteral(Struct value, int precision, TemporalPrecisionLossHandlingMode handlingMode) {
        final long picoseconds = longValue(value, StructuredTemporal.PICOSECONDS_FIELD);
        StructuredTemporalPreflightValidator.reduceFraction(picoseconds, precision, handlingMode);

        // A day-time duration is a single signed number of seconds, even if its components have different signs
        BigDecimal totalSeconds = BigDecimal.valueOf(intValue(value, StructuredTemporal.DAYS_FIELD)).multiply(BigDecimal.valueOf(86_400))
                .add(BigDecimal.valueOf(intValue(value, StructuredTemporal.HOURS_FIELD)).multiply(BigDecimal.valueOf(3_600)))
                .add(BigDecimal.valueOf(intValue(value, StructuredTemporal.MINUTES_FIELD)).multiply(BigDecimal.valueOf(60)))
                .add(BigDecimal.valueOf(longValue(value, StructuredTemporal.SECONDS_FIELD)))
                .add(BigDecimal.valueOf(picoseconds, StructuredTemporal.MAX_FRACTIONAL_SECOND_PRECISION));
        final RoundingMode roundingMode = handlingMode == TemporalPrecisionLossHandlingMode.ROUND ? RoundingMode.HALF_UP : RoundingMode.DOWN;
        totalSeconds = totalSeconds.setScale(precision, roundingMode);

        final boolean negative = totalSeconds.signum() < 0;
        BigDecimal remaining = totalSeconds.abs();
        final long days = remaining.divideToIntegralValue(BigDecimal.valueOf(86_400)).longValueExact();
        remaining = remaining.remainder(BigDecimal.valueOf(86_400));
        final int hours = remaining.divideToIntegralValue(BigDecimal.valueOf(3_600)).intValueExact();
        remaining = remaining.remainder(BigDecimal.valueOf(3_600));
        final int minutes = remaining.divideToIntegralValue(BigDecimal.valueOf(60)).intValueExact();
        remaining = remaining.remainder(BigDecimal.valueOf(60));

        final int wholeSeconds = remaining.intValue();
        final int fraction = remaining.remainder(BigDecimal.ONE).movePointRight(precision).intValue();
        final String suffix = StructuredTemporalSupport.fixedFraction(fraction, precision);
        return String.format("%s%d %02d:%02d:%02d%s",
                negative ? "-" : "",
                days,
                hours,
                minutes,
                wholeSeconds,
                suffix);
    }

    private int intValue(Struct value, String fieldName) {
        final Integer fieldValue = value.getInt32(fieldName);
        return fieldValue == null ? 0 : fieldValue;
    }

    private long longValue(Struct value, String fieldName) {
        final Long fieldValue = value.getInt64(fieldName);
        return fieldValue == null ? 0 : fieldValue;
    }

    private static boolean isPositive(String value) {
        return Integer.parseInt(value) > 0;
    }
}
