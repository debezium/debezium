/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.AbstractConnectStructType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code STRUCT} schema types that no dedicated handler
 * is registered for. StarRocks cannot create a column for an arbitrary Connect STRUCT, so the
 * value is serialized to JSON and stored in a StarRocks {@code JSON} column, mirroring how
 * {@code MAP} schema types are handled by {@link MapToJsonType}.
 */
class StructToJsonType extends AbstractConnectStructType {

    public static final StructToJsonType INSTANCE = new StructToJsonType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return JsonType.INSTANCE.getQueryBinding(column, schema, value);
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return JsonType.INSTANCE.getTypeName(schema, isKey);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Struct struct) {
            value = structToJsonString(struct);
        }
        return JsonType.INSTANCE.bind(index, schema, value);
    }
}
