/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.AbstractGeometryType;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;

public class GeometryType extends AbstractGeometryType {

    public static final JdbcType INSTANCE = new GeometryType();

    protected static final String GEO_FROM_WKB_FUNCTION = "%s.ST_GeomFromWKB(?, ?)";
    private static final String TYPE_NAME = "%s.geometry";

    protected String postgisSchema = "public";

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);

        if (config instanceof JdbcSinkConnectorConfig jdbcConfig) {
            this.postgisSchema = jdbcConfig.getPostgresPostgisSchema();
        }
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return String.format(GEO_FROM_WKB_FUNCTION, postgisSchema);
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return String.format(TYPE_NAME, postgisSchema);
    }
}
