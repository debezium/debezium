/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredZonedTimestamp;

/**
 * An implementation of {@link JdbcType} for {@link StructuredZonedTimestamp} values.
 */
public class StructuredZonedTimestampType extends AbstractTimestampType {

    public static final StructuredZonedTimestampType INSTANCE = new StructuredZonedTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return StructuredZonedTimestamp.schemaNames();
    }

    @Override
    protected int getJdbcType() {
        return Types.TIMESTAMP_WITH_TIMEZONE;
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
        final var struct = requireStruct(value);
        StructuredTemporalPreflightValidator.validateZonedTimestamp(
                struct, getDialect().getTargetTemporalCapabilities());
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        final int offsetSeconds = offsetSeconds(struct);
        return getDialect().getFormattedDateTime(StructuredTemporalSupport.toOffsetDateTime(
                struct, getSchemaTimestampPrecision(schema), getPrecisionLossHandlingMode(),
                capabilities.targetZonedTimestampRange(null, offsetSeconds), getRangeLossHandlingMode(), targetDescription(schema)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (getDialect() == null) {
            return List.of(new ValueBindDescriptor(index, StructuredTemporalSupport.toOffsetDateTime(requireStruct(value)), getJdbcType()));
        }
        final var struct = requireStruct(value);
        final OffsetDateTime clamped = clampIfOutOfRange(struct);
        if (clamped != null) {
            return List.of(new ValueBindDescriptor(index, clamped, getJdbcType()));
        }
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        StructuredTemporalPreflightValidator.validateZonedTimestamp(struct, capabilities);
        final OffsetDateTime offsetDateTime = StructuredTemporalSupport.toOffsetDateTime(
                struct, getSchemaTimestampPrecision(schema), getPrecisionLossHandlingMode(),
                capabilities.targetZonedTimestampRange(null, offsetSeconds(struct)), getRangeLossHandlingMode(), targetDescription(schema));
        return List.of(new ValueBindDescriptor(index, offsetDateTime, getJdbcType()));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null && clampIfOutOfRange(requireStruct(value)) == null) {
            final var struct = requireStruct(value);
            final var capabilities = getDialect().getTargetTemporalCapabilities();
            StructuredTemporalPreflightValidator.validateZonedTimestamp(struct, capabilities);
            StructuredTemporalPreflightValidator.validatePrecision(
                    column, struct, capabilities.targetTimestampPrecision(column), getPrecisionLossHandlingMode());
            StructuredTemporalSupport.adjustTimestamp(
                    struct, capabilities.targetTimestampPrecision(column), getPrecisionLossHandlingMode(),
                    capabilities.targetZonedTimestampRange(column, offsetSeconds(struct)), getRangeLossHandlingMode(),
                    targetDescription(column));
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final var struct = requireStruct(value);
        final OffsetDateTime clamped = clampIfOutOfRange(struct);
        if (clamped != null) {
            return List.of(new ValueBindDescriptor(index, clamped, getJdbcType()));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimestampPrecision(column);
        final OffsetDateTime offsetDateTime = StructuredTemporalSupport.toOffsetDateTime(
                struct, precision, getPrecisionLossHandlingMode(),
                getDialect().getTargetTemporalCapabilities().targetZonedTimestampRange(column, offsetSeconds(struct)),
                getRangeLossHandlingMode(), targetDescription(column));
        return List.of(new ValueBindDescriptor(index, offsetDateTime, getJdbcType()));
    }

    /**
     * Returns the clamped date-time when {@code timestamp.clamp.out.of.range.values} is enabled and the
     * structured value is a finite, valid calendar value that lies outside the dialect's supported range;
     * otherwise returns {@code null} so that the value keeps the structured temporal precision and range
     * handling.
     */
    protected OffsetDateTime clampIfOutOfRange(Struct struct) {
        if (!isClampOutOfRangeValuesEnabled() || !StructuredTemporal.isFinite(struct)) {
            return null;
        }
        final OffsetDateTime offsetDateTime;
        try {
            offsetDateTime = StructuredTemporalSupport.toOffsetDateTime(struct);
        }
        catch (DateTimeException e) {
            return null;
        }
        final OffsetDateTime clamped = clampIfOutOfRange(offsetDateTime);
        return clamped.equals(offsetDateTime) ? null : clamped;
    }

    private static int offsetSeconds(org.apache.kafka.connect.data.Struct value) {
        final Integer offsetSeconds = value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD);
        return offsetSeconds == null ? 0 : offsetSeconds;
    }
}
