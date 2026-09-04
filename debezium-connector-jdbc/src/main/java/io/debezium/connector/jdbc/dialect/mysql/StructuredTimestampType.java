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
 * MySQL implementation of {@link io.debezium.time.StructuredTimestamp} values.
 */
public class StructuredTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + StructuredTemporalLiteral.timestamp(requireStruct(value)) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.timestamp(requireStruct(value)), Types.VARCHAR));
    }
}
