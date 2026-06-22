/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.sql.Types;
import java.util.List;
import java.util.Locale;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;

/**
 * Oracle implementation of {@link StructuredDuration} values.
 */
public class StructuredDurationType extends AbstractType {

    public static final StructuredDurationType INSTANCE = new StructuredDurationType();

    private enum IntervalKind {
        YEAR_MONTH,
        DAY_SECOND,
        TEXT
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredDuration.SCHEMA_NAME };
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
            case DAY_SECOND -> "TO_DSINTERVAL('" + toDaySecondLiteral(struct) + "')";
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

    private String getDaySecondTypeName(Schema schema) {
        final String dayPrecision = getSourceColumnSize(schema)
                .filter(StructuredDurationType::isPositive)
                .map(value -> "(" + value + ")")
                .orElse("");
        final int secondPrecision = getSourceColumnPrecision(schema).map(Integer::parseInt).orElse(9);
        return "INTERVAL DAY" + dayPrecision + " TO SECOND(" + Math.min(secondPrecision, 9) + ")";
    }

    private String toBindingValue(Schema schema, Struct value) {
        return switch (resolveIntervalKind(schema)) {
            case YEAR_MONTH -> toYearMonthLiteral(value);
            case DAY_SECOND -> toDaySecondLiteral(value);
            case TEXT -> StructuredTemporalSupport.toDurationString(value);
        };
    }

    private IntervalKind resolveIntervalKind(Schema schema) {
        if (schema != null) {
            return resolveIntervalKind(getSourceColumnType(schema).orElse(null));
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
        final int years = intValue(value, StructuredTemporal.YEARS_FIELD);
        final int months = intValue(value, StructuredTemporal.MONTHS_FIELD);
        final boolean negative = years < 0 || months < 0;
        return String.format("%s%d-%02d", negative ? "-" : "", Math.abs(years), Math.abs(months));
    }

    private String toDaySecondLiteral(Struct value) {
        final int days = intValue(value, StructuredTemporal.DAYS_FIELD);
        final int hours = intValue(value, StructuredTemporal.HOURS_FIELD);
        final int minutes = intValue(value, StructuredTemporal.MINUTES_FIELD);
        final long seconds = longValue(value, StructuredTemporal.SECONDS_FIELD);
        final int nanos = intValue(value, StructuredTemporal.NANOS_FIELD);
        final boolean negative = days < 0 || hours < 0 || minutes < 0 || seconds < 0 || nanos < 0;
        final String fraction = nanos == 0 ? "" : "." + String.format("%09d", Math.abs(nanos));
        return String.format("%s%d %02d:%02d:%02d%s",
                negative ? "-" : "",
                Math.abs(days),
                Math.abs(hours),
                Math.abs(minutes),
                Math.abs(seconds),
                fraction);
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
