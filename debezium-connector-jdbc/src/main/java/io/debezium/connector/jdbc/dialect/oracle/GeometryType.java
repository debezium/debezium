/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractGeoType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.geometry.Geometry;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;

public class GeometryType extends AbstractGeoType {

    public static final JdbcType INSTANCE = new GeometryType();

    private static final String TYPE_NAME = "MDSYS.SDO_GEOMETRY";

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "SDO_GEOMETRY(TO_BLOB(?), ?)";
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Geometry.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return TYPE_NAME;
    }

}
