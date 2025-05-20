/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.field;

import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An immutable representation of a {@link org.apache.kafka.connect.data.Field} in a {@link org.apache.kafka.connect.sink.SinkRecord}.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
@Immutable
public class JdbcFieldDescriptor extends FieldDescriptor {

    private final Type type;

    // Lazily prepared
    private String queryBinding;

    public JdbcFieldDescriptor(FieldDescriptor fieldDescriptor, Type type, boolean isKey) {
        super(fieldDescriptor.getSchema(), fieldDescriptor.getName(), isKey);
        this.type = type;
    }

    public String getQueryBinding(ColumnDescriptor column, Object value) {
        if (queryBinding == null) {
            queryBinding = type.getQueryBinding(column, schema, value);
        }
        return queryBinding;
    }

    public List<ValueBindDescriptor> bind(int startIndex, Object value) {
        return type.bind(startIndex, schema, value);
    }

    @Override
    public String toString() {
        return "JdbcFieldDescriptor{" +
                "schema=" + schema +
                ", name='" + name + '\'' +
                ", isKey='" + isKey + '\'' +
                ", typeName='" + type.getTypeName(schema, isKey) + '\'' +
                ", type=" + type +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}
