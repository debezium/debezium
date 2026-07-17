/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Db2 binding for structured timestamps, using a server-side cast so fractional precision up to picoseconds is not
 * reduced by the standard JDBC temporal types.
 */
public class StructuredTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredTimestampType {

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimestampPrecision(column);
        return String.format("cast(? as timestamp(%d))", precision);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + toLiteral(schema, requireStruct(value)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, toLiteral(schema, requireStruct(value)), Types.VARCHAR));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimestampPrecision(column);
        return List.of(new ValueBindDescriptor(index,
                StructuredTemporalSupport.toLocalDateTimeLiteral(
                        requireStruct(value), precision, getPrecisionLossHandlingMode(),
                        getDialect().getTargetTemporalCapabilities().targetTimestampRange(column), getRangeLossHandlingMode(),
                        targetDescription(column)),
                Types.VARCHAR));
    }

    protected String toLiteral(Schema schema, org.apache.kafka.connect.data.Struct value) {
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        return StructuredTemporalSupport.toLocalDateTimeLiteral(
                value, getSchemaTimestampPrecision(schema), getPrecisionLossHandlingMode(),
                capabilities.targetTimestampRange(null), getRangeLossHandlingMode(), targetDescription(schema));
    }
}
