/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.AbstractConnectStructType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code STRUCT} schema types that no dedicated handler
 * is registered for. PostgreSQL cannot create a column for an arbitrary Connect STRUCT, so the
 * value is serialized to JSON and stored in a native {@code jsonb} column, mirroring how {@code MAP}
 * schema types are handled. Types registered by schema name (e.g. geometry) keep resolving through
 * their dedicated handlers first.
 */
class StructToJsonbType extends AbstractConnectStructType {

    public static final StructToJsonbType INSTANCE = new StructToJsonbType();

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
        if (value instanceof Struct struct) {
            value = structToJsonString(struct);
        }
        return List.of(new ValueBindDescriptor(index, value));
    }
}
