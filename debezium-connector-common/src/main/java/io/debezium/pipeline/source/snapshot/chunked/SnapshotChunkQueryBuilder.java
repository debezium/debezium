/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Builds SQL queries for snapshot chunks with boundary conditions.
 *
 * @author Chris Cranford
 */
public class SnapshotChunkQueryBuilder {

    private final JdbcConnection jdbcConnection;
    private final ChunkQueryBuilder<DataCollectionId> connectionChunkQueryBuilder;

    public SnapshotChunkQueryBuilder(JdbcConnection jdbcConnection, RelationalDatabaseConnectorConfig config) {
        this.jdbcConnection = jdbcConnection;
        this.connectionChunkQueryBuilder = jdbcConnection.chunkQueryBuilder(config);

    }

    /**
     * Build a SELECT query for a chunk with appropriate WHERE clause.
     *
     * @param chunk The snapshot chunk
     * @param keyColumns The key columns used for chunking
     * @param baseSelect The base select statement (without WHERE for boundaries)
     * @return Complete SELECT statement with chunk boundary conditions
     */
    public String buildChunkQuery(SnapshotChunk chunk, List<Column> keyColumns, String baseSelect) {
        // For single chunk (no boundaries), use base select
        if (!chunk.hasLowerBound() && !chunk.hasUpperBound()) {
            return baseSelect;
        }

        final StringBuilder whereClause = new StringBuilder();

        // Add lower bound: key >= lowerBound
        if (chunk.hasLowerBound()) {
            connectionChunkQueryBuilder.addLowerBound(keyColumns, chunk.getLowerBounds(), whereClause, true);
        }

        // Add upper bound: key < upperBound (or key <= upperBound for last chunk)
        if (chunk.hasUpperBound()) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            connectionChunkQueryBuilder.addUpperBound(keyColumns, chunk.getUpperBounds(), whereClause, chunk.isLastChunk());
        }

        return injectWhereClause(baseSelect, whereClause.toString(), keyColumns);
    }

    /**
     * Inject WHERE clause into base select, adding ORDER BY for key columns.
     */
    private String injectWhereClause(String baseSelect, String whereClause, List<Column> keyColumns) {
        final String upperSelect = baseSelect.toUpperCase();
        final int whereIndex = upperSelect.indexOf(" WHERE ");
        final int orderByIndex = upperSelect.indexOf(" ORDER BY ");

        final StringBuilder result = new StringBuilder();

        // Build ORDER BY clause
        final String orderBy = String.join(", ", keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .toList());

        if (whereIndex >= 0) {
            // Existing WHERE - add with AND
            result.append(baseSelect, 0, whereIndex + 7);
            result.append("(").append(whereClause).append(") AND ");
            if (orderByIndex >= 0) {
                result.append(baseSelect, whereIndex + 7, orderByIndex);
                result.append(" ORDER BY ").append(orderBy);
            }
            else {
                result.append(baseSelect.substring(whereIndex + 7));
                result.append(" ORDER BY ").append(orderBy);
            }
        }
        else if (orderByIndex >= 0) {
            // No WHERE but has ORDER BY
            result.append(baseSelect, 0, orderByIndex);
            result.append(" WHERE ").append(whereClause);
            result.append(" ORDER BY ").append(orderBy);
        }
        else {
            // No WHERE, no ORDER BY
            result.append(baseSelect);
            result.append(" WHERE ").append(whereClause);
            result.append(" ORDER BY ").append(orderBy);
        }

        return result.toString();
    }

    /**
     * Prepare a statement and bind chunk boundary parameters.
     */
    public PreparedStatement prepareChunkStatement(SnapshotChunk chunk, List<Column> keyColumns, String sql) throws SQLException {
        final PreparedStatement statement = jdbcConnection.connection().prepareStatement(sql);
        if (!chunk.hasLowerBound() && !chunk.hasUpperBound()) {
            return statement;
        }
        int paramIndex = 1;

        // Bind lower bound parameters
        if (chunk.hasLowerBound()) {
            paramIndex = connectionChunkQueryBuilder.bindBoundaryParams(
                    statement, keyColumns, chunk.getLowerBounds(), paramIndex, jdbcConnection);
        }

        // Bind upper bound parameters
        if (chunk.hasUpperBound()) {
            connectionChunkQueryBuilder.bindBoundaryParams(
                    statement, keyColumns, chunk.getUpperBounds(), paramIndex, jdbcConnection);
        }

        return statement;
    }

}
