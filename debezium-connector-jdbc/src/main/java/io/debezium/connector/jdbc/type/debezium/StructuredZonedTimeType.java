/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.OffsetTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredZonedTime;

/**
 * An implementation of {@link JdbcType} for {@link StructuredZonedTime} values.
 */
public class StructuredZonedTimeType extends AbstractTimeType {

    public static final StructuredZonedTimeType INSTANCE = new StructuredZonedTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredZonedTime.SCHEMA_NAME };
    }

    @Override
    protected int getTimePrecision(Schema schema) {
        return StructuredTemporalSupport.getPrecision(schema)
                .stream()
                .map(precision -> Math.min(precision, getDialect().getMaxTimePrecision()))
                .findFirst()
                .orElse(-1);
    }

    @Override
    protected boolean shouldUseSourcePrecision(int precision, int maxPrecision) {
        return precision >= 0 && precision <= maxPrecision;
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final Struct struct = requireStruct(value);
        final OffsetTime offsetTime = OffsetTime.of(
                StructuredTemporalSupport.toLocalTime(struct),
                java.time.ZoneOffset.ofTotalSeconds(struct.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)));
        return List.of(new ValueBindDescriptor(index, offsetTime, Types.TIME_WITH_TIMEZONE));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null) {
            final var capabilities = getDialect().getTargetTemporalCapabilities();
            StructuredTemporalPreflightValidator.validatePrecision(
                    column, requireStruct(value), capabilities.targetTimePrecision(column), getPrecisionLossHandlingMode());
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final Struct struct = requireStruct(value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimePrecision(column);
        final OffsetTime offsetTime = OffsetTime.of(
                StructuredTemporalSupport.toLocalTime(struct, precision, getPrecisionLossHandlingMode()),
                java.time.ZoneOffset.ofTotalSeconds(struct.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)));
        return List.of(new ValueBindDescriptor(index, offsetTime, Types.TIME_WITH_TIMEZONE));
    }
}
