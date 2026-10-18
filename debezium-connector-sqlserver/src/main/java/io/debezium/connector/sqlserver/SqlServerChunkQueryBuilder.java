/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.DefaultChunkQueryBuilder;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * SQL Server incremental snapshot chunk queries.
 * <p>
 * {@code datetime} stores 1/300s ticks. JDBC binds millisecond values as {@code datetime2}, so chunk
 * bounds for that type are compared as {@code datetime} (dbz#2871).
 */
public class SqlServerChunkQueryBuilder<T extends DataCollectionId>
        extends DefaultChunkQueryBuilder<T> {

    public SqlServerChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                      JdbcConnection jdbcConnection) {
        super(config, jdbcConnection);
    }

    @Override
    protected String boundPlaceholder(Column column) {
        if ("datetime".equalsIgnoreCase(column.typeName())) {
            return "CAST(? AS datetime)";
        }
        return super.boundPlaceholder(column);
    }
}
