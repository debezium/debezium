/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Point;

/**
 * An implementation of {@link Type} for {@code io.debezium.data.geometry.Point} types.
 *
 * @author Chris Cranford
 */
class PointType extends AbstractType {

    public static final PointType INSTANCE = new PointType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "io.debezium.data.geometry.Point" };
    }

    @Override
    public String getQueryBinding(Schema schema) {
        return "cast(? as point)";
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "point";
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else {
            final Struct struct = (Struct) value;
            final double x = struct.getFloat64(Point.X_FIELD);
            final double y = struct.getFloat64(Point.Y_FIELD);
            query.setParameter(index, String.format("(%f,%f)", x, y));
        }
    }
}
