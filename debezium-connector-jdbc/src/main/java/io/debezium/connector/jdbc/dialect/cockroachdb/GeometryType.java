/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.AbstractGeometryType;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.geometry.Geometry} types.
 * CockroachDB provides the spatial types and {@code ST_GeomFromWKB} natively, so unlike the
 * PostgreSQL dialect no PostGIS extension schema qualification is used.
 *
 * @author Virag Tripathi
 */
class GeometryType extends AbstractGeometryType {

    public static final GeometryType INSTANCE = new GeometryType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "ST_GeomFromWKB(?, ?)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "geometry";
    }

}
