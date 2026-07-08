/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.AbstractConnectMapType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code MAP} schema types. The PostgreSQL dialect maps
 * these to {@code hstore}, which CockroachDB does not provide; the map is serialized to JSON and
 * stored in CockroachDB's native {@code jsonb} type instead.
 *
 * @author Virag Tripathi
 */
class MapToJsonbType extends AbstractConnectMapType {

    public static final MapToJsonbType INSTANCE = new MapToJsonbType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as jsonb)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "jsonb";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Map) {
            value = mapToJsonString(value);
        }
        return List.of(new ValueBindDescriptor(index, value));
    }

}
