/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * MySQL implementation of {@link io.debezium.time.StructuredDate} values.
 */
public class StructuredDateType extends io.debezium.connector.jdbc.type.debezium.StructuredDateType {

    public static final StructuredDateType INSTANCE = new StructuredDateType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (getDialect() == null) {
            return "'" + StructuredTemporalLiteral.date(requireStruct(value)) + "'";
        }
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return "'" + StructuredTemporalLiteral.date(
                requireStruct(value), capabilities.targetDateRange(null), getRangeLossHandlingMode(), targetDescription(schema)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (getDialect() == null) {
            return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.date(requireStruct(value)), Types.VARCHAR));
        }
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.date(
                requireStruct(value), capabilities.targetDateRange(null), getRangeLossHandlingMode(), targetDescription(schema)), Types.VARCHAR));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null) {
            StructuredTemporalLiteral.date(
                    requireStruct(value), getDialect().getTargetTemporalCapabilities().targetDateRange(column),
                    getRangeLossHandlingMode(), targetDescription(column));
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.date(
                requireStruct(value), getDialect().getTargetTemporalCapabilities().targetDateRange(column),
                getRangeLossHandlingMode(), targetDescription(column)), Types.VARCHAR));
    }
}
