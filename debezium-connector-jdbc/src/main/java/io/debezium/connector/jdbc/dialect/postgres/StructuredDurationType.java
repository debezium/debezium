/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.type.AbstractTemporalType;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;

/**
 * PostgreSQL implementation of {@link StructuredDuration} values.
 */
public class StructuredDurationType extends AbstractTemporalType {

    public static final StructuredDurationType INSTANCE = new StructuredDurationType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredDuration.SCHEMA_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as interval)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "interval";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return String.format("'%s'", toIntervalLiteral(requireStruct(value)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, toIntervalLiteral(requireStruct(value)), Types.VARCHAR));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return;
        }
        final Struct struct = requireStruct(value);
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        StructuredTemporalPreflightValidator.validateDuration(schema, struct, capabilities);
        StructuredTemporalPreflightValidator.validatePrecision(
                column, struct, capabilities.targetTimePrecision(column), getPrecisionLossHandlingMode());
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimePrecision(column);
        return List.of(new ValueBindDescriptor(index,
                toIntervalLiteral(requireStruct(value), precision, getPrecisionLossHandlingMode()), Types.VARCHAR));
    }

    private String toIntervalLiteral(Struct value) {
        return toIntervalLiteral(value, null, TemporalPrecisionLossHandlingMode.FAIL);
    }

    private String toIntervalLiteral(Struct value, Integer precision, TemporalPrecisionLossHandlingMode handlingMode) {
        final StringBuilder builder = new StringBuilder();
        append(builder, value.getInt32(StructuredTemporal.YEARS_FIELD), " years");
        append(builder, value.getInt32(StructuredTemporal.MONTHS_FIELD), " months");
        append(builder, value.getInt32(StructuredTemporal.DAYS_FIELD), " days");
        append(builder, value.getInt32(StructuredTemporal.HOURS_FIELD), " hours");
        append(builder, value.getInt32(StructuredTemporal.MINUTES_FIELD), " minutes");

        final Long seconds = value.getInt64(StructuredTemporal.SECONDS_FIELD);
        final Long picoseconds = value.getInt64(StructuredTemporal.PICOSECONDS_FIELD);
        BigDecimal fractionalSeconds = BigDecimal.valueOf(seconds == null ? 0 : seconds)
                .add(BigDecimal.valueOf(picoseconds == null ? 0 : picoseconds, 12));
        if (precision != null) {
            final RoundingMode roundingMode = handlingMode == TemporalPrecisionLossHandlingMode.ROUND ? RoundingMode.HALF_UP : RoundingMode.DOWN;
            fractionalSeconds = fractionalSeconds.setScale(precision, roundingMode);
        }
        fractionalSeconds = fractionalSeconds.stripTrailingZeros();
        if (fractionalSeconds.signum() != 0 || builder.length() == 0) {
            append(builder, fractionalSeconds.toPlainString(), " seconds");
        }
        return builder.toString().trim();
    }

    private void append(StringBuilder builder, Integer value, String suffix) {
        if (value != null && value != 0) {
            append(builder, value.toString(), suffix);
        }
    }

    private void append(StringBuilder builder, String value, String suffix) {
        if (builder.length() > 0) {
            builder.append(' ');
        }
        builder.append(value).append(suffix);
    }
}
