/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Explicitly disables geospatial logical types for SingleStore until the binding
 * semantics are validated with SingleStore integration coverage.
 */
class UnsupportedGeospatialType extends AbstractType {

    public static final UnsupportedGeospatialType INSTANCE = new UnsupportedGeospatialType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME, Geography.LOGICAL_NAME, Point.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        throw unsupported(schema);
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        throw unsupported(schema);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        throw unsupported(schema);
    }

    private ConnectException unsupported(Schema schema) {
        return new ConnectException(String.format("SingleStore dialect does not support geospatial schema type '%s'", schema.name()));
    }
}
