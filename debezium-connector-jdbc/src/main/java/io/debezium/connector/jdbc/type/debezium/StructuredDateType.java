/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDate;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractDateType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDate;

/**
 * An implementation of {@link JdbcType} for {@link StructuredDate} values.
 */
public class StructuredDateType extends AbstractDateType {

    public static final StructuredDateType INSTANCE = new StructuredDateType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredDate.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return getDialect().getFormattedDate(StructuredTemporalSupport.toLocalDate(
                requireStruct(value), capabilities.targetDateRange(null), getRangeLossHandlingMode(), targetDescription(schema)));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (getDialect() == null) {
            return List.of(new ValueBindDescriptor(index, StructuredTemporalSupport.toLocalDate(requireStruct(value))));
        }
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        final LocalDate localDate = StructuredTemporalSupport.toLocalDate(
                requireStruct(value), capabilities.targetDateRange(null), getRangeLossHandlingMode(), targetDescription(schema));
        return List.of(new ValueBindDescriptor(index, localDate));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null) {
            final var range = getDialect().getTargetTemporalCapabilities().targetDateRange(column);
            StructuredTemporalSupport.adjustDate(
                    requireStruct(value), range, getRangeLossHandlingMode(), targetDescription(column));
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final var range = getDialect().getTargetTemporalCapabilities().targetDateRange(column);
        final LocalDate localDate = StructuredTemporalSupport.toLocalDate(
                requireStruct(value), range, getRangeLossHandlingMode(), targetDescription(column));
        return List.of(new ValueBindDescriptor(index, localDate));
    }

}
