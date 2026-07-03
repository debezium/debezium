/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Line;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.geometry.Line} values, binding
 * them to the native PostgreSQL {@code line} type.
 *
 * @author Debezium Authors
 */
class LineType extends AbstractType {

    public static final LineType INSTANCE = new LineType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Line.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as line)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "line";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        final Struct line = requireStruct(value);
        final double a = line.getFloat64(Line.A_FIELD);
        final double b = line.getFloat64(Line.B_FIELD);
        final double c = line.getFloat64(Line.C_FIELD);
        return List.of(new ValueBindDescriptor(index, "{" + a + "," + b + "," + c + "}"));
    }
}
