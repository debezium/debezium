/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code STRUCT} schema types that no dedicated handler
 * is registered for, mapped to the dialect's connect string-based type as a JSON text value,
 * mirroring how {@code MAP} schema types fall back to {@link ConnectMapToConnectStringType}.
 * Dialects with a native JSON column type override this with their own {@code STRUCT} handler,
 * and types registered by schema name (e.g. geometry) keep resolving through their dedicated
 * handlers first.
 */
public class ConnectStructToConnectStringType extends AbstractConnectStructType {

    public static final ConnectStructToConnectStringType INSTANCE = new ConnectStructToConnectStringType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return ConnectStringType.INSTANCE.getQueryBinding(column, schema, value);
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return ConnectStringType.INSTANCE.getTypeName(schema, isKey);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return ConnectStringType.INSTANCE.getDefaultValueBinding(schema, value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Struct struct) {
            value = structToJsonString(struct);
        }
        return ConnectStringType.INSTANCE.bind(index, schema, value);
    }
}
