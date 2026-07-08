/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Geography;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.geometry.Geography} types
 * backed by CockroachDB's native {@code geography} type.
 *
 * @author Virag Tripathi
 */
class GeographyType extends GeometryType {

    public static final GeographyType INSTANCE = new GeographyType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geography.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(ST_GeomFromWKB(?, ?) as geography)";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "geography";
    }

}
