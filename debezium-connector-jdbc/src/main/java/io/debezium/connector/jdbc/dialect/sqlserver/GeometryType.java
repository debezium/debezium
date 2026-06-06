/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.AbstractGeometryType;
import io.debezium.sink.column.ColumnDescriptor;

/**
 *
 * @author Chris Cranford
 */
public class GeometryType extends AbstractGeometryType {

    public static final GeometryType INSTANCE = new GeometryType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "geometry::STGeomFromWKB(?, ?)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "GEOMETRY";
    }

}
