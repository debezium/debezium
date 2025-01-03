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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChunkQueryBuilder.class);

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
            final Object[] maximumKey = context.maximumKey().get();
            final Object[] chunkEndPosition = context.chunkEndPosititon();
            final StringBuilder sql = new StringBuilder();
            // Window boundaries
            addLowerBound(context, table, chunkEndPosition, sql);
            // Table boundaries
            sql.append(" AND ");
            addUpperBound(context, table, maximumKey, sql);
            condition = sql.toString();
        }
        final List<Column> queryColumns = getQueryColumns(context, table);
        if (jdbcConnection.nullsSortLast().isEmpty() && queryColumns.stream().anyMatch(Column::isOptional)) {
            // You need to override nullsSortLast on JdbcConnection for your connector if you want to be able to chunk based on nullable columns.
            throw new UnsupportedOperationException("The sort order of NULL values in the incremental snapshot key is unknown.");
        }
        final String orderBy = queryColumns.stream()
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
        TableId tableId = table.id();
        return table.columns().stream()
                .filter(column -> !connectorConfig.isColumnsFiltered() || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), column.name()))
                .map(column -> jdbcConnection.quotedColumnIdString(column.name()))
                .collect(Collectors.joining(", "));
    }

    protected void addLowerBound(IncrementalSnapshotContext<T> context, Table table, Object[] boundaryKey, StringBuilder sql) {
        // To make window boundaries working for more than one column it is necessary to calculate
        // with independently increasing values in each column independently.
        // For one column the condition will be (? will always be the last value seen for the given column)
        // (k1 > ?)
        // For two columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?)
        // For four columns
        // (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 > ?) OR (k1 = ? AND k2 = ? AND k3 = ? AND k4 > ?)
        // etc.
        //
        // Special considerations are needed when a column is nullable. First, the ORDER BY clause will return NULL values
        // in different sort orders depending on the database, so jdbcConnection.nullsSortLast() helps us figure that out.
        // Next, each individual comparison as shown in the simple non-NULLABLE example above is translated as follows:
        //
        // Databases where NULL sorts last (e.g. PostgreSQL):
        // (k1 > ?) where ? is non-NULL: (k1 > ? OR k1 IS NULL): find bigger values; NULL is considered bigger & not seen yet
        // (k1 > ?) where ? is NULL: (FALSE): by definition, NULL is the last value and no value can possibly be higher
        // (k1 = ?) where ? is non-NULL: (k1 = ?): no translation needed for non-NULL values
        // (k1 = ?) where ? is NULL: (k1 IS NULL): need to use IS NULL instead of equality comparison
        //
        // Databases where NULL sorts first (e.g. MySQL):
        // (k1 > ?) where ? is non-NULL: (k1 > ? AND k1 IS NOT NULL): find bigger values; NULL is smaller & already seen
        // (k1 > ?) where ? is NULL: (k1 IS NOT NULL): by definition, NULL is the first value and every value is always higher
        // (k1 = ?) where ? is non-NULL: (k1 = ?): no translation needed for non-NULL values
        // (k1 = ?) where ? is NULL: (k1 IS NULL): need to use IS NULL instead of equality comparison
        final List<Column> pkColumns = getQueryColumns(context, table);
        final Optional<Boolean> nullsSortLast = jdbcConnection.nullsSortLast();
        if (pkColumns.size() > 1) {
            sql.append('(');
        }
        for (int i = 0; i < pkColumns.size(); i++) {
            final boolean isLastIterationForI = (i == pkColumns.size() - 1);
            sql.append('(');
            for (int j = 0; j < i + 1; j++) {
                final boolean isLastIterationForJ = (i == j);
                final String pkColumnName = jdbcConnection.quotedColumnIdString(pkColumns.get(j).name());
                if (pkColumns.get(j).isRequired()) {
                    sql.append(pkColumnName);
                    sql.append(isLastIterationForJ ? " > ?" : " = ?");
                }
                else if (boundaryKey[j] != null) {
                    if (isLastIterationForJ) {
                        sql.append('(');
                        sql.append(pkColumnName);
                        sql.append(" > ?");
                        if (nullsSortLast.get()) {
                            sql.append(" OR ");
                            sql.append(pkColumnName);
                            sql.append(" IS NULL)");
                        }
                        else {
                            sql.append(" AND ");
                            sql.append(pkColumnName);
                            sql.append(" IS NOT NULL)");
                        }
                    }
                    else {
                        sql.append(pkColumnName);
                        sql.append(" = ?");
                    }
                }
                else {
                    if (isLastIterationForJ) {
                        // Identifies values greater than NULL based on the database sorting behavior
                        if (nullsSortLast.get()) {
                            // Basically a FALSE literal: works around lack of FALSE literal in some databases, like Oracle
                            sql.append("1 = 0"); // nothing is greater than NULL
                        }
                        else {
                            sql.append(pkColumnName);
                            sql.append(" IS NOT NULL"); // everything is greater than NULL
                        }
                    }
                    else {
                        sql.append(pkColumnName);
                        sql.append(" IS NULL");
                    }
                }
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

    protected void addUpperBound(IncrementalSnapshotContext<T> context, Table table, Object[] boundaryKey, StringBuilder sql) {
        sql.append("NOT ");
        addLowerBound(context, table, boundaryKey, sql);
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
                    if (chunkEndPosition[j] != null) {
                        jdbcConnection.setQueryColumnValue(statement, queryColumns.get(j), ++pos, chunkEndPosition[j]);
                    }
                }
            }
            // Fill maximum key placeholders
            for (int i = 0; i < maximumKey.length; i++) {
                for (int j = 0; j < i + 1; j++) {
                    if (maximumKey[j] != null) {
                        jdbcConnection.setQueryColumnValue(statement, queryColumns.get(j), ++pos, maximumKey[j]);
                    }
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
        String selectWithRowLimits = jdbcConnection.buildSelectWithRowLimits(table.id(), 1, buildProjection(table), Optional.empty(),
                additionalCondition, orderBy);
        LOGGER.debug("MaxPrimaryKeyQuery {}", selectWithRowLimits);
        return selectWithRowLimits;
    }

    private KeyMapper getKeyMapper() {
        return connectorConfig.getKeyMapper() == null ? table -> table.primaryKeyColumns() : connectorConfig.getKeyMapper();
    }

    @Override
    public List<Column> getQueryColumns(IncrementalSnapshotContext<T> context, Table table) {
        if (context != null && context.currentDataCollectionId() != null) {
            Optional<String> surrogateKey = context.currentDataCollectionId().getSurrogateKey();
            if (surrogateKey.isPresent()) {
                Column column = table.columnWithName(surrogateKey.get());
                if (column == null) {
                    throw new IllegalArgumentException("Surrogate key \"" + surrogateKey.get() + "\" doesn't exist in table \"" + table.id() + "\"");
                }
                return Collections.singletonList(column);
            }
        }
        return getKeyMapper().getKeyKolumns(table);
    }
}
