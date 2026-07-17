/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Stores structured local times as text because StarRocks does not provide a {@code TIME} type.
 */
class StructuredTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredTimeType {

    static final StructuredTimeType INSTANCE = new StructuredTimeType();

    private static final int MAX_PRECISION = 12;

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "varchar(21)";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + toLiteral(value) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        return bind(index, value);
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value != null) {
            toLiteral(value);
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        return bind(index, value);
    }

    private List<ValueBindDescriptor> bind(int index, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, toLiteral(value), Types.VARCHAR));
    }

    private String toLiteral(Object value) {
        return StructuredTemporalSupport.toTimeLiteral(requireStruct(value), MAX_PRECISION, getPrecisionLossHandlingMode());
    }
}
