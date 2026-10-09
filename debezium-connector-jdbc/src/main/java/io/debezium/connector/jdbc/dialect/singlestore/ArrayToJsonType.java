/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code ARRAY} schema types that are mapped to
 * a SingleStore {@code JSON} column type.
 */
class ArrayToJsonType extends AbstractType {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final ArrayToJsonType INSTANCE = new ArrayToJsonType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "ARRAY" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        // SingleStore accepts a JSON string bound to the parameter for JSON columns.
        return "?";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return JsonType.INSTANCE.getTypeName(schema, isKey);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof List) {
            try {
                value = OBJECT_MAPPER.writeValueAsString(value);
            }
            catch (JsonProcessingException e) {
                throw new ConnectException("Failed to serialize ARRAY data to JSON", e);
            }
        }
        return JsonType.INSTANCE.bind(index, schema, value);
    }

}
