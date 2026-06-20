/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.sink.column.ColumnDescriptor;

/**
 * PostgreSQL implementation of {@link io.debezium.time.StructuredZonedTime} values.
 */
public class StructuredZonedTimeType extends io.debezium.connector.jdbc.type.debezium.StructuredZonedTimeType {

    public static final StructuredZonedTimeType INSTANCE = new StructuredZonedTimeType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as timetz)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "timetz";
    }
}
