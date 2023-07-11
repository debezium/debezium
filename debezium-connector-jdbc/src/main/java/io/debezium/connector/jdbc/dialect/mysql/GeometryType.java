/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractGeoType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Geometry;

public class GeometryType extends AbstractGeoType {

    public static final Type INSTANCE = new GeometryType();

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema) {
        return "ST_GeomFromWKB(?, ?)";
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "geometry";
    }
}
