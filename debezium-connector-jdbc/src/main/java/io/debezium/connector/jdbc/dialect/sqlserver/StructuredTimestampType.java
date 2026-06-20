/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import org.apache.kafka.connect.data.Schema;

import io.debezium.sink.column.ColumnDescriptor;

/**
 * SQL Server implementation of {@link io.debezium.time.StructuredTimestamp} values.
 */
public class StructuredTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as datetime2(7))";
    }
}
