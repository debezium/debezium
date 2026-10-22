/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredTimestamp;

/**
 * An implementation of {@link JdbcType} for {@link StructuredTimestamp} values.
 */
public class StructuredTimestampType extends AbstractTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return StructuredTimestamp.schemaNames();
    }

    @Override
    protected int getTimePrecision(Schema schema) {
        return StructuredTemporalSupport.getPrecision(schema)
                .stream()
                .map(precision -> Math.min(precision, getDialect().getMaxTimestampPrecision()))
                .findFirst()
                .orElse(-1);
    }

    @Override
    protected boolean shouldUseSourcePrecision(int precision, int maxPrecision) {
        return precision >= 0 && precision <= maxPrecision;
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return getDialect().getFormattedDateTime(StructuredTemporalSupport.toLocalDateTime(
                requireStruct(value), getSchemaTimestampPrecision(schema), getPrecisionLossHandlingMode(),
                capabilities.targetTimestampRange(null), getRangeLossHandlingMode(), targetDescription(schema)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (getDialect() == null) {
            return List.of(new ValueBindDescriptor(index, StructuredTemporalSupport.toLocalDateTime(requireStruct(value)), getJdbcType()));
        }
        final Struct struct = requireStruct(value);
        final LocalDateTime clamped = clampIfOutOfRange(struct);
        if (clamped != null) {
            return List.of(new ValueBindDescriptor(index, clamped, getJdbcType()));
        }
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        final int precision = getSchemaTimestampPrecision(schema);
        final LocalDateTime localDateTime = StructuredTemporalSupport.toLocalDateTime(
                struct, precision, getPrecisionLossHandlingMode(), capabilities.targetTimestampRange(null),
                getRangeLossHandlingMode(), targetDescription(schema));
        return List.of(new ValueBindDescriptor(index, localDateTime, getJdbcType()));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null && clampIfOutOfRange(requireStruct(value)) == null) {
            final var capabilities = getDialect().getTargetTemporalCapabilities();
            StructuredTemporalPreflightValidator.validatePrecision(
                    column, requireStruct(value), capabilities.targetTimestampPrecision(column), getPrecisionLossHandlingMode());
            StructuredTemporalSupport.adjustTimestamp(
                    requireStruct(value), capabilities.targetTimestampPrecision(column), getPrecisionLossHandlingMode(),
                    capabilities.targetTimestampRange(column), getRangeLossHandlingMode(), targetDescription(column));
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final LocalDateTime clamped = clampIfOutOfRange(requireStruct(value));
        if (clamped != null) {
            return List.of(new ValueBindDescriptor(index, clamped, getJdbcType()));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimestampPrecision(column);
        final LocalDateTime localDateTime = StructuredTemporalSupport.toLocalDateTime(
                requireStruct(value), precision, getPrecisionLossHandlingMode(),
                getDialect().getTargetTemporalCapabilities().targetTimestampRange(column), getRangeLossHandlingMode(),
                targetDescription(column));
        return List.of(new ValueBindDescriptor(index, localDateTime, getJdbcType()));
    }

    /**
     * Returns the clamped date-time when {@code timestamp.clamp.out.of.range.values} is enabled and the
     * structured value is a finite, valid calendar value that lies outside the dialect's supported range;
     * otherwise returns {@code null} so that the value keeps the structured temporal precision and range
     * handling.
     */
    protected LocalDateTime clampIfOutOfRange(Struct struct) {
        if (!isClampOutOfRangeValuesEnabled() || !StructuredTemporal.isFinite(struct)) {
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
}
