/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.DefaultChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Builds the queries for reading incremental snapshot chunks from a MariaDB table.
 * <p>
 * A {@code YEAR(2)} column is read as {@code YEAR(column)}, so that the chunk reports the same year as the
 * binlog. The columns the chunks are ordered by are left untouched, because MariaDB compares a {@code YEAR(2)}
 * column by its two-digit value, not by the year {@code YEAR(column)} returns.
 */
public class MariaDbChunkQueryBuilder<T extends DataCollectionId> extends DefaultChunkQueryBuilder<T> {

    public MariaDbChunkQueryBuilder(RelationalDatabaseConnectorConfig config, JdbcConnection jdbcConnection) {
        super(config, jdbcConnection);
    }

    @Override
    protected String buildProjection(IncrementalSnapshotContext<T> context, Table table) {
        final List<Column> chunkColumns = getQueryColumns(context, table);
        final List<Column> projectedColumns = getProjectedColumns(table);
        final Set<Column> year2Columns = projectedColumns.stream()
                .filter(column -> Year2Column.isYear2(column) && !chunkColumns.contains(column))
                .collect(Collectors.toSet());
        if (year2Columns.isEmpty()) {
            return super.buildProjection(context, table);
        }
        return projectedColumns.stream()
                .map(column -> year2Columns.contains(column)
                        ? Year2Column.selectExpression(jdbcConnection, column)
                        : jdbcConnection.quoteIdentifier(column.name()))
                .collect(Collectors.joining(", "));
    }
}
