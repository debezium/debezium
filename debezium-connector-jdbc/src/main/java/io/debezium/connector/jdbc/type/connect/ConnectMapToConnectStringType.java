/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code MAP} schema types that are mapped to the
 * dialect's connect string-based type.
 *
 * @author Chris Cranford
 */
public class ConnectMapToConnectStringType extends AbstractConnectMapType {

    public static final ConnectMapToConnectStringType INSTANCE = new ConnectMapToConnectStringType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return ConnectStringType.INSTANCE.getQueryBinding(column, schema, value);
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return ConnectStringType.INSTANCE.getTypeName(dialect, schema, key);
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return ConnectStringType.INSTANCE.getDefaultValueBinding(dialect, schema, value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Map) {
            value = mapToJsonString(value);
        }
        return ConnectStringType.INSTANCE.bind(index, schema, value);
    }

}
