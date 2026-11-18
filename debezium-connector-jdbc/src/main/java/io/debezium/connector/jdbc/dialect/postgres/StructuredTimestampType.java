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

/**
 * PostgreSQL implementation of {@link io.debezium.time.StructuredTimestamp} values.
 */
public class StructuredTimestampType extends io.debezium.connector.jdbc.type.debezium.StructuredTimestampType {

    public static final StructuredTimestampType INSTANCE = new StructuredTimestampType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as timestamp)";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return PostgresInfinity.defaultValueBinding(supportsInfinity(), value)
                .orElseGet(() -> super.getDefaultValueBinding(schema, value));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        return PostgresInfinity.bind(supportsInfinity(), index, value).orElseGet(() -> super.bind(index, schema, value));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (!PostgresInfinity.isStoredAsIs(supportsInfinity(), value)) {
            super.validate(column, schema, value);
        }
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        return PostgresInfinity.bind(supportsInfinity(), index, value).orElseGet(() -> super.bind(index, column, schema, value));
    }

    private boolean supportsInfinity() {
        return getDialect() != null && getDialect().getTargetTemporalCapabilities().timestampInfinitySupported();
    }
}
