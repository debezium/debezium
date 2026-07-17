/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;

/**
 * MySQL implementation of {@link io.debezium.time.StructuredTimestamp} values.
 */
public class StructuredTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return "'" + StructuredTemporalLiteral.timestamp(
                requireStruct(value), getSchemaTimestampPrecision(schema), getPrecisionLossHandlingMode(),
                capabilities.targetTimestampRange(null), getRangeLossHandlingMode(), targetDescription(schema)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final Struct struct = requireStruct(value);
        final LocalDateTime clamped = clampIfOutOfRange(struct);
        if (clamped != null) {
            return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.timestamp(clamped), Types.VARCHAR));
        }
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.timestamp(
                struct, getSchemaTimestampPrecision(schema), getPrecisionLossHandlingMode(),
                capabilities.targetTimestampRange(null), getRangeLossHandlingMode(), targetDescription(schema)), Types.VARCHAR));
    }

    /**
     * Returns the clamped date-time when the structured value is a finite, valid calendar value that
     * lies outside the dialect's supported range; otherwise returns {@code null} so that the value is
     * bound from its raw components, preserving MySQL's tolerance for invalid calendar literals.
     */
    private LocalDateTime clampIfOutOfRange(Struct struct) {
        if (!StructuredTemporal.isFinite(struct)) {
            return null;
        }
        final LocalDateTime localDateTime;
        try {
            localDateTime = StructuredTemporalSupport.toLocalDateTime(struct);
        }
        catch (DateTimeException e) {
            return null;
        }
        final LocalDateTime clamped = clampIfOutOfRange(localDateTime);
        return clamped.equals(localDateTime) ? null : clamped;
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null) {
            final var capabilities = getDialect().getTargetTemporalCapabilities();
            final int precision = capabilities.targetTimestampPrecision(column);
            StructuredTemporalPreflightValidator.validatePrecision(
                    column, requireStruct(value), precision, getPrecisionLossHandlingMode());
            StructuredTemporalLiteral.timestamp(
                    requireStruct(value), precision, getPrecisionLossHandlingMode(),
                    capabilities.targetTimestampRange(column), getRangeLossHandlingMode(), targetDescription(column));
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimestampPrecision(column);
        return List.of(new ValueBindDescriptor(index,
                StructuredTemporalLiteral.timestamp(
                        requireStruct(value), precision, getPrecisionLossHandlingMode(),
                        getDialect().getTargetTemporalCapabilities().targetTimestampRange(column), getRangeLossHandlingMode(),
                        targetDescription(column)),
                Types.VARCHAR));
    }
}
