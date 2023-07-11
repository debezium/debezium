/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractGeoType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.geometry.Geometry;

public class GeometryType extends AbstractGeoType {

    public static final Type INSTANCE = new GeometryType();

    static final String GEO_FROM_WKB_FUNCTION = "%s.ST_GeomFromWKB(?, ?)";
    private static final String TYPE_NAME = "%s.geometry";
    String postgisSchema;

    @Override
    public void configure(JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);

        this.postgisSchema = config.getPostgresPostgisSchema();
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return value == null ? "?" : String.format(GEO_FROM_WKB_FUNCTION, postgisSchema);
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return String.format(TYPE_NAME, postgisSchema);
    }
}
