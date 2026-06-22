/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

class MicroTimeType extends io.debezium.connector.jdbc.type.debezium.MicroTimeType {

    public static final MicroTimeType INSTANCE = new MicroTimeType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        if (PostgresTimeBoundary.isBoundaryMicroseconds(value)) {
            return PostgresTimeBoundary.TIME_QUERY_BINDING;
        }
        return super.getQueryBinding(column, schema, value);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (PostgresTimeBoundary.isBoundaryMicroseconds(value)) {
            return "'" + PostgresTimeBoundary.BOUNDARY_TIME + "'";
        }
        return super.getDefaultValueBinding(schema, value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (PostgresTimeBoundary.isBoundaryMicroseconds(value)) {
            return List.of(new ValueBindDescriptor(index, PostgresTimeBoundary.BOUNDARY_TIME));
        }
        return super.bind(index, schema, value);
    }
}
