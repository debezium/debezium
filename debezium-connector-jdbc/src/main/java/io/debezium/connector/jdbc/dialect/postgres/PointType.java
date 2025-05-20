/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Point;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link Type} for {@code io.debezium.data.geometry.Point} types.
 *
 * @author Chris Cranford
 */
class PointType extends GeometryType {

    public static final PointType INSTANCE = new PointType();

    private static final String TYPE_NAME = "point";

    private static final String GEO_FROM_WKB_FUNCTION_AS_POINT = "cast(" + GEO_FROM_WKB_FUNCTION + " as point)";

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return String.format(GEO_FROM_WKB_FUNCTION_AS_POINT, postgisSchema);
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Point.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return TYPE_NAME;
    }
}
