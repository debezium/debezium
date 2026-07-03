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
import io.debezium.data.geometry.Circle;
import io.debezium.data.geometry.Point;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.geometry.Circle} values, binding
 * them to the native PostgreSQL {@code circle} type without requiring PostGIS.
 *
 * @author Debezium Authors
 */
class CircleType extends AbstractType {

    public static final CircleType INSTANCE = new CircleType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Circle.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as circle)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "circle";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        final Struct circle = requireStruct(value);
        final Struct center = circle.getStruct(Circle.CENTER_FIELD);
        final double x = center.getFloat64(Point.X_FIELD);
        final double y = center.getFloat64(Point.Y_FIELD);
        final double radius = circle.getFloat64(Circle.RADIUS_FIELD);
        return List.of(new ValueBindDescriptor(index, "<(" + x + "," + y + ")," + radius + ">"));
    }
}
