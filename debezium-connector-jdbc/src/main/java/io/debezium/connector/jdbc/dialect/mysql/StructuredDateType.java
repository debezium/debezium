/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * MySQL implementation of {@link io.debezium.time.StructuredDate} values.
 */
public class StructuredDateType extends io.debezium.connector.jdbc.type.debezium.StructuredDateType {

    public static final StructuredDateType INSTANCE = new StructuredDateType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + StructuredTemporalLiteral.date(requireStruct(value)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.date(requireStruct(value)), Types.VARCHAR));
    }
}
