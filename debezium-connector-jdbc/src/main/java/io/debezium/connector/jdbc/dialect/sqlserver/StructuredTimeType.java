/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * SQL Server implementation of {@link io.debezium.time.StructuredTime} values.
 */
public class StructuredTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredTimeType {

    public static final StructuredTimeType INSTANCE = new StructuredTimeType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as time(7))";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        final LocalDateTime localDateTime = StructuredTemporalSupport.toLocalTime(requireStruct(value)).atDate(LocalDate.EPOCH);
        return List.of(new ValueBindDescriptor(index, localDateTime));
    }
}
