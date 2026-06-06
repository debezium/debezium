/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.spatial.GeometryBytes;

public abstract class AbstractGeometryType extends AbstractType {

    public static final String SRID = "srid";
    public static final String WKB = "wkb";

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME, Geography.LOGICAL_NAME };
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        final GeometryBytes geometry = getGeometry(value);
        if (geometry == null) {
            return getNullGeometryBinding(index);
        }
        else if (geometry.isEmptyGeometryCollection()) {
            return getEmptyGeometryCollectionBinding(index, geometry);
        }
        return getGeometryBinding(index, geometry);
    }

    protected List<ValueBindDescriptor> getNullGeometryBinding(int index) {
        return List.of(new ValueBindDescriptor(index, null), new ValueBindDescriptor(index + 1, null));
    }

    protected List<ValueBindDescriptor> getEmptyGeometryCollectionBinding(int index, GeometryBytes geometry) {
        return getGeometryBinding(index, geometry);
    }

    protected List<ValueBindDescriptor> getGeometryBinding(int index, GeometryBytes geometry) {
        return List.of(new ValueBindDescriptor(index, geometry.getBytes()), new ValueBindDescriptor(index + 1, geometry.getSrid()));
    }

    protected GeometryBytes getGeometry(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Struct struct) {
            // Default srid is 0 for both
            // MySQL https://dev.mysql.com/doc/refman/8.0/en/spatial-reference-systems.html#:~:text=The%20SRS%20denoted%20in%20MySQL,for%20spatial%20data%20in%20MySQL.
            // PostgreSQL https://postgis.net/docs/using_postgis_dbmanagement.html#spatial_ref_sys_table
            final Integer srid = Optional.ofNullable(struct.getInt32(SRID)).orElse(0);

            GeometryBytes geometry = new GeometryBytes(struct.getBytes(WKB), srid);
            if (useExtendedWkb() && !geometry.isExtended()) {
                geometry = geometry.asExtendedWkb();
            }
            else if (!useExtendedWkb() && geometry.isExtended()) {
                geometry = geometry.asWkb();
            }

            return geometry;
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected boolean useExtendedWkb() {
        return false;
    }
}
