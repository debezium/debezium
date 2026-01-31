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
import io.debezium.relational.Column;

/**
 * Builds SQL queries for snapshot chunks with boundary conditions.
 *
 * @author Chris Cranford
 */
public class SnapshotChunkQueryBuilder {

    private final JdbcConnection jdbcConnection;

    public SnapshotChunkQueryBuilder(JdbcConnection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
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
            addLowerBound(keyColumns, whereClause);
        }

        // Add upper bound: key < upperBound (or key <= upperBound for last chunk)
        if (chunk.hasUpperBound()) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            addUpperBound(keyColumns, whereClause, chunk.isLastChunk());
        }

        return injectWhereClause(baseSelect, whereClause.toString(), keyColumns);
    }

    /**
     * Add lower bound condition: (k1, k2, ...) >= (?, ?, ...)
     * For composite keys, uses row value constructor syntax or cascading OR conditions
     * depending on database support.
     */
    protected void addLowerBound(List<Column> keyColumns, StringBuilder sql) {
        if (keyColumns.size() == 1) {
            final String colName = jdbcConnection.quoteIdentifier(keyColumns.get(0).name());
            sql.append(colName).append(" >= ?");
        }
        else {
            addCompositeLowerBound(keyColumns, sql);
        }
    }

    /**
     * Add upper bound condition: (k1, k2, ...) < (?, ?, ...)
     */
    protected void addUpperBound(List<Column> keyColumns, StringBuilder sql, boolean inclusive) {
        if (keyColumns.size() == 1) {
            final String colName = jdbcConnection.quoteIdentifier(keyColumns.get(0).name());
            sql.append(colName).append(inclusive ? " <= ?" : " < ?");
        }
        else {
            addCompositeUpperBound(keyColumns, sql, inclusive);
        }
    }

    /**
     * Build composite key lower bound using cascading OR conditions.
     * Pattern: (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 >= ?)
     */
    private void addCompositeLowerBound(List<Column> keyColumns, StringBuilder sql) {
        final List<String> quotedColumnNames = keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .toList();

        sql.append('(');
        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append('(');
            for (int j = 0; j <= i; j++) {
                if (j > 0) {
                    sql.append(" AND ");
                }
                final String colName = quotedColumnNames.get(j);
                if (j == i) {
                    // Last column in this term: use > (or >= for final term)
                    final String operator = (i == keyColumns.size() - 1) ? " >= ?" : " > ?";
                    sql.append(colName).append(operator);
                }
                else {
                    sql.append(colName).append(" = ?");
                }
            }
            sql.append(')');
        }
        sql.append(')');
    }

    /**
     * Build composite key upper bound using cascading OR conditions.
     * Pattern: (k1 < ?) OR (k1 = ? AND k2 < ?) OR (k1 = ? AND k2 = ? AND k3 < ?)
     */
    private void addCompositeUpperBound(List<Column> keyColumns, StringBuilder sql, boolean inclusive) {
        final List<String> quotedColumnNames = keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .toList();

        final String operator = inclusive ? " <= ?" : " < ?";

        sql.append('(');
        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append('(');
            for (int j = 0; j < i; j++) {
                sql.append(quotedColumnNames.get(j)).append(" = ? AND ");
            }
            sql.append(quotedColumnNames.get(i)).append(operator);
            sql.append(')');
        }
        sql.append(')');
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
            paramIndex = bindCompositeBoundary(statement, keyColumns, chunk.getLowerBounds(), paramIndex);
        }

        // Bind upper bound parameters
        if (chunk.hasUpperBound()) {
            bindCompositeBoundary(statement, keyColumns, chunk.getUpperBounds(), paramIndex);
        }

        return statement;
    }

    /**
     * Bind parameters for composite key boundary.
     * Pattern: (k1 > ?) OR (k1 = ? AND k2 > ?) OR ...
     * Params: v1, v1, v2, v1, v2, v3, ...
     */
    private int bindCompositeBoundary(PreparedStatement statement, List<Column> keyColumns, Object[] boundaryValues, int startIndex) throws SQLException {
        int paramIndex = startIndex;

        if (keyColumns.size() == 1) {
            // Single column: just one parameter
            jdbcConnection.setQueryColumnValue(statement, keyColumns.get(0), paramIndex++, boundaryValues[0]);
        }
        else {
            // Composite: bind for each term in the OR pattern
            for (int i = 0; i < keyColumns.size(); i++) {
                for (int j = 0; j <= i; j++) {
                    jdbcConnection.setQueryColumnValue(statement, keyColumns.get(j), paramIndex++, boundaryValues[j]);
                }
            }
        }

        return paramIndex;
    }

}
