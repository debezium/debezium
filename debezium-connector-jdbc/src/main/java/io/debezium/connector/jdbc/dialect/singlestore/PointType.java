/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.data.geometry.Point;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.spatial.GeometryBytes;

/**
 * An implementation for {@link Point} logical values that maps to SingleStore {@code GEOGRAPHYPOINT}.
 */
class PointType extends GeometryType {

    public static final PointType INSTANCE = new PointType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Point.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "geographypoint";
    }

    @Override
    protected List<ValueBindDescriptor> getGeometryBinding(int index, GeometryBytes geometry) {
        final mil.nga.wkb.geom.Geometry value = readGeometry(geometry);
        if (!(value instanceof mil.nga.wkb.geom.Point)) {
            throw unsupportedGeometry(value.getGeometryType().getName());
        }
        return bindWkt(index, toWkt(value));
    }
}
