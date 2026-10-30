/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.pipeline.source.snapshot.incremental.DefaultChunkQueryBuilder;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Builds the chunk queries for the SQL Server incremental snapshots.
 * <p>
 * SQL Server omits the {@code HIDDEN} columns of a table from the result of {@code SELECT *}, while the JDBC
 * driver reports them in the metadata, so the schema of such a table holds more columns than the chunk query
 * returns. Such a table is therefore read with an explicit list of its columns, the same way the initial snapshot
 * reads it. Every other table keeps the {@code SELECT *} projection, as does a table whose columns are filtered,
 * because the base class then already projects the filtered columns explicitly.
 */
public class SqlServerChunkQueryBuilder<T extends DataCollectionId> extends DefaultChunkQueryBuilder<T> {

    private final SqlServerConnection connection;

    /**
     * Creates a new chunk query builder.
     *
     * @param config the connector configuration
     * @param connection the connection used to read the hidden columns of a table
     */
    public SqlServerChunkQueryBuilder(RelationalDatabaseConnectorConfig config, SqlServerConnection connection) {
        super(config, connection);
        this.connection = connection;
    }

    @Override
    protected String buildProjection(Table table) {
        if (connectorConfig.isColumnsFiltered() || !hasHiddenColumns(table)) {
            return super.buildProjection(table);
        }
        return table.columns().stream()
                .map(column -> jdbcConnection.quoteIdentifier(column.name()))
                .collect(Collectors.joining(", "));
    }

    private boolean hasHiddenColumns(Table table) {
        final Set<String> hiddenColumnNames = connection.getHiddenColumns(table.id());
        return table.columns().stream().anyMatch(column -> hiddenColumnNames.contains(column.name()));
    }
}
