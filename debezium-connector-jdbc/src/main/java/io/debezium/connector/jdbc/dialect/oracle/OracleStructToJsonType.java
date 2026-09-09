/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.AbstractConnectStructType;
import io.debezium.connector.jdbc.type.connect.ConnectStringType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code STRUCT} schema types that no dedicated,
 * schema-named handler is registered for, targeting the native Oracle {@code JSON} column type that
 * is available in Oracle 21c and later. The struct is serialized to JSON and stored in a
 * {@code JSON} column; the serialized value is bound exactly as the string-based fallback binds it.
 * On Oracle 19 and earlier the dialect keeps using {@code ConnectStructToConnectStringType}, which
 * stores the same JSON text in a string/CLOB column.
 */
class OracleStructToJsonType extends AbstractConnectStructType {

    public static final OracleStructToJsonType INSTANCE = new OracleStructToJsonType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return ConnectStringType.INSTANCE.getQueryBinding(column, schema, value);
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "json";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Struct struct) {
            value = structToJsonString(struct);
        }
        return ConnectStringType.INSTANCE.bind(index, schema, value);
    }
}
