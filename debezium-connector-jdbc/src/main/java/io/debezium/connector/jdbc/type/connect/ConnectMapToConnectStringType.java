/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

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
    public String getTypeName(Schema schema, boolean isKey) {
        return ConnectStringType.INSTANCE.getTypeName(schema, isKey);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return ConnectStringType.INSTANCE.getDefaultValueBinding(schema, value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Map) {
            value = mapToJsonString(value);
        }
        return ConnectStringType.INSTANCE.bind(index, schema, value);
    }

}
