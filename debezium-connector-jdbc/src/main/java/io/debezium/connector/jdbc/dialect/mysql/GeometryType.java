/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractGeoType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Geometry;
import io.debezium.sink.column.ColumnDescriptor;

public class GeometryType extends AbstractGeoType {

    public static final JdbcType INSTANCE = new GeometryType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "ST_GeomFromWKB(?, ?)";
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "geometry";
    }
}
