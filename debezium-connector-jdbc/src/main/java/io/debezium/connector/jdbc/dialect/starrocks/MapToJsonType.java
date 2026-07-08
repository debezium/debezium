/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.AbstractConnectMapType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code MAP} schema types that are mapped to a
 * StarRocks {@code JSON} column type.
 */
class MapToJsonType extends AbstractConnectMapType {

    public static final MapToJsonType INSTANCE = new MapToJsonType();

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
        if (value instanceof Map) {
            value = mapToJsonString(value);
        }
        return JsonType.INSTANCE.bind(index, schema, value);
    }
}
