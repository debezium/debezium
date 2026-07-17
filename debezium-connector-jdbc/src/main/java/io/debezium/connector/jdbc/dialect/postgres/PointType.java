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
import io.debezium.data.geometry.Point;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.geometry.Point} values, binding
 * them to the native PostgreSQL {@code point} type without requiring PostGIS.
 *
 * @author Chris Cranford
 */
class PointType extends AbstractType {

    public static final PointType INSTANCE = new PointType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Point.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as point)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "point";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        final Struct point = requireStruct(value);
        final double x = point.getFloat64(Point.X_FIELD);
        final double y = point.getFloat64(Point.Y_FIELD);
        return List.of(new ValueBindDescriptor(index, "(" + x + "," + y + ")"));
    }
}
