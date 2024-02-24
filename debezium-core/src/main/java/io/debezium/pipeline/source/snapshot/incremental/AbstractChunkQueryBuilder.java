/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Base class for building queries for reading incremental snapshot chunks from a table.
 */
public abstract class AbstractChunkQueryBuilder<T extends DataCollectionId>
        implements ChunkQueryBuilder<T> {

    protected final RelationalDatabaseConnectorConfig connectorConfig;
    protected final JdbcConnection jdbcConnection;
    protected ColumnNameFilter columnFilter;

    public AbstractChunkQueryBuilder(RelationalDatabaseConnectorConfig config,
                                     JdbcConnection jdbcConnection) {
        this.connectorConfig = config;
        this.jdbcConnection = jdbcConnection;
        this.columnFilter = config.getColumnFilter();
    }

    @Override
    public String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition) {
        return buildChunkQuery(context, table, connectorConfig.getIncrementalSnapshotChunkSize(), additionalCondition);
    }

    @Override
    public String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, int limit, Optional<String> additionalCondition) {
        String condition = null;
        // Add condition when this is not the first query
        if (context.isNonInitialChunk()) {
            final StringBuilder sql = new StringBuilder();
            // Window boundaries
            addLowerBound(context, table, sql);
            // Table boundaries
            sql.append(" AND NOT ");
            addLowerBound(context, table, sql);
            condition = sql.toString();
        }
        final String orderBy = getQueryColumns(context, table).stream()
                .map(c -> jdbcConnection.quotedColumnIdString(c.name()))
                .collect(Collectors.joining(", "));
        return jdbcConnection.buildSelectWithRowLimits(table.id(),
                limit,
                buildProjection(table),
                Optional.ofNullable(condition),
                additionalCondition,
                orderBy);
    }

    protected String buildProjection(Table table) {
        String projection = "*";
        if (connectorConfig.isColumnsFiltered()) {
            TableId tableId = table.id();
            projection = table.columns().stream()
                    .filter(column -> columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), column.name()))
                    .map(column -> jdbcConnection.quotedColumnIdString(column.name()))
                    .collect(Collectors.joining(", "));
        }
        return projection;
    }

    private void addLowerBound(IncrementalSnapshotContext<T> context, Table table, StringBuilder sql) {
        // To make window boundaries working for more than one column it is necessary to calculate
        // with independently increasing values in each column independently.
        // For one column the condition will be (? will always be the last value seen for the given column)
        // (k1 > ?)
        // For two columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?)
        // For four columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 > ?) OR (k1 = ? AND k2 = ? AND k3 = ? AND k4 > ?)
        // etc.
        final List<Column> pkColumns = getQueryColumns(context, table);
        if (pkColumns.size() > 1) {
            sql.append('(');
        }
        for (int i = 0; i < pkColumns.size(); i++) {
            final boolean isLastIterationForI = (i == pkColumns.size() - 1);
            sql.append('(');
            for (int j = 0; j < i + 1; j++) {
                final boolean isLastIterationForJ = (i == j);
                sql.append(jdbcConnection.quotedColumnIdString(pkColumns.get(j).name()));
                sql.append(isLastIterationForJ ? " > ?" : " = ?");
                if (!isLastIterationForJ) {
                    sql.append(" AND ");
                }
            }
            sql.append(")");
            if (!isLastIterationForI) {
                sql.append(" OR ");
            }
        }
        if (pkColumns.size() > 1) {
            sql.append(')');
        }
    }

    @Override
    public PreparedStatement readTableChunkStatement(IncrementalSnapshotContext<T> context, Table table, String sql) throws SQLException {
        final PreparedStatement statement = jdbcConnection.readTablePreparedStatement(connectorConfig, sql,
                OptionalLong.empty());
        if (context.isNonInitialChunk()) {
            final Object[] maximumKey = context.maximumKey().get();
            final Object[] chunkEndPosition = context.chunkEndPosititon();
            // Fill boundaries placeholders
            int pos = 0;
            final List<Column> queryColumns = getQueryColumns(context, table);
            for (int i = 0; i < chunkEndPosition.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    jdbcConnection.setQueryColumnValue(statement, queryColumns.get(j), ++pos, chunkEndPosition[j]);
                }
            }
            // Fill maximum key placeholders
            for (int i = 0; i < chunkEndPosition.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    jdbcConnection.setQueryColumnValue(statement, queryColumns.get(j), ++pos, maximumKey[j]);
                }
            }
        }
        return statement;
    }

    @Override
    public String buildMaxPrimaryKeyQuery(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition) {
        final String orderBy = getQueryColumns(context, table).stream()
                .map(c -> jdbcConnection.quotedColumnIdString(c.name()))
                .collect(Collectors.joining(" DESC, ")) + " DESC";
        return jdbcConnection.buildSelectWithRowLimits(table.id(), 1, buildProjection(table), Optional.empty(),
                additionalCondition, orderBy);
    }

    private KeyMapper getKeyMapper() {
        return connectorConfig.getKeyMapper() == null ? table -> table.primaryKeyColumns() : connectorConfig.getKeyMapper();
    }

    @Override
    public List<Column> getQueryColumns(IncrementalSnapshotContext<T> context, Table table) {
        if (context != null && context.currentDataCollectionId() != null) {
            Optional<String> surrogateKey = context.currentDataCollectionId().getSurrogateKey();
            if (surrogateKey.isPresent()) {
                return Collections.singletonList(table.columnWithName(surrogateKey.get()));
            }
        }
        return getKeyMapper().getKeyKolumns(table);
    }
}

