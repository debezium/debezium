/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;

/**
 * PostgreSQL implementation of {@link io.debezium.time.StructuredDate} values.
 */
public class StructuredDateType extends io.debezium.connector.jdbc.type.debezium.StructuredDateType {

    public static final StructuredDateType INSTANCE = new StructuredDateType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as date)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Struct struct) {
            if (StructuredTemporal.isPositiveInfinity(struct)) {
                return List.of(new ValueBindDescriptor(index, "infinity", Types.VARCHAR));
            }
            if (StructuredTemporal.isNegativeInfinity(struct)) {
                return List.of(new ValueBindDescriptor(index, "-infinity", Types.VARCHAR));
            }
        }
        return super.bind(index, schema, value);
    }
}
