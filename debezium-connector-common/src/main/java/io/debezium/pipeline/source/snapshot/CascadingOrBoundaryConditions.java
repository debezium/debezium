/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;

/**
 * Utility methods for building cascading-OR boundary conditions used in chunked SQL queries.
 *
 * <p>For a composite key {@code (k1, k2, k3)}, the cascading-OR lower bound pattern is:
 * <pre>
 *   (k1 > ?) OR (k1 = ? AND k2 > ?) OR (k1 = ? AND k2 = ? AND k3 op ?)
 * </pre>
 * where {@code op} is {@code >=} for an inclusive final term or {@code >} for exclusive.
 *
 * <p>The corresponding parameter binding will bind values in a triangular pattern, once per column
 * per OR term, where a 3-column key requires 1+2+3 = 6 parameter slots per boundary.
 *
 * @author Chris Cranford
 */
public final class CascadingOrBoundaryConditions {

    private CascadingOrBoundaryConditions() {
    }

    /**
     * Appends the cascading-OR lower-bound condition for the given (already-quoted) column names.
     *
     * <p>For a single column: {@code k1 >= ?} or {@code k1 > ?}.
     * <p>For multiple columns: {@code (k1 > ?) OR (k1 = ? AND k2 > ?) OR ... OR (k1 = ? AND ... AND kN op ?)}.
     *
     * @param columnNames quoted column name strings in key order
     * @param sql target string builder
     * @param inclusiveFinal {@code true} to use {@code >=} on the final (most-specific) term;
     *                       {@code false} to use {@code >} throughout
     */
    public static void buildLowerBound(List<String> columnNames, StringBuilder sql, boolean inclusiveFinal) {
        if (columnNames.size() == 1) {
            sql.append(columnNames.get(0)).append(inclusiveFinal ? " >= ?" : " > ?");
            return;
        }
        sql.append('(');
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append('(');
            for (int j = 0; j <= i; j++) {
                if (j > 0) {
                    sql.append(" AND ");
                }
                final String col = columnNames.get(j);
                if (j == i) {
                    final boolean isLastTerm = (i == columnNames.size() - 1);
                    sql.append(col).append((isLastTerm && inclusiveFinal) ? " >= ?" : " > ?");
                }
                else {
                    sql.append(col).append(" = ?");
                }
            }
            sql.append(')');
        }
        sql.append(')');
    }

    /**
     * Appends the cascading-OR upper-bound condition for the given (already-quoted) column names.
     *
     * <p>For a single column: {@code k1 <= ?} or {@code k1 < ?}.
     * <p>For multiple columns: {@code (k1 op ?) OR (k1 = ? AND k2 op ?) OR ...}.
     *
     * @param columnNames quoted column name strings in key order
     * @param sql target string builder
     * @param inclusive {@code true} to use {@code <=}; {@code false} to use {@code <}
     */
    public static void buildUpperBound(List<String> columnNames, StringBuilder sql, boolean inclusive) {
        final String operator = inclusive ? " <= ?" : " < ?";
        if (columnNames.size() == 1) {
            sql.append(columnNames.get(0)).append(operator);
            return;
        }
        sql.append('(');
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append('(');
            for (int j = 0; j < i; j++) {
                sql.append(columnNames.get(j)).append(" = ? AND ");
            }
            sql.append(columnNames.get(i)).append(operator);
            sql.append(')');
        }
        sql.append(')');
    }

    /**
     * Binds values in the triangular parameter pattern for a cascading-OR boundary condition.
     * None of the values may be {@code null}.
     *
     * <p>For a 3-column key and {@code values = [v1, v2, v3]}, binds in order:
     * {@code v1, v1 v2, v1 v2 v3} (six parameters total).
     *
     * @param statement the prepared statement to bind into
     * @param columns the key columns, in key order
     * @param values boundary values, must be non-null and the same length as {@code cols}
     * @param startIndex the 1-based index of the first parameter slot to use
     * @param connection connection used for type-aware binding
     * @return the next available (unused) parameter index
     * @throws SQLException if binding fails
     */
    public static int bindTriangularParams(PreparedStatement statement, List<Column> columns, Object[] values, int startIndex, JdbcConnection connection)
            throws SQLException {
        int paramIndex = startIndex;
        for (int i = 0; i < columns.size(); i++) {
            for (int j = 0; j <= i; j++) {
                connection.setQueryColumnValue(statement, columns.get(j), paramIndex++, values[j]);
            }
        }
        return paramIndex;
    }

    /**
     * Binds values in the triangular parameter pattern, skipping {@code null} entries.
     *
     * <p>Used when the SQL was generated with {@code IS NULL} / {@code IS NOT NULL} literals for
     * null-valued columns (which require no {@code ?} placeholder).
     *
     * @param statement the prepared statement to bind into
     * @param columns the key columns, in key order
     * @param values boundary values; {@code null} entries are skipped
     * @param startIndex the 1-based index of the first parameter slot to use
     * @param connection connection used for type-aware binding
     * @return the next available (unused) parameter index
     * @throws SQLException if binding fails
     */
    public static int bindTriangularParamsSkipNulls(PreparedStatement statement, List<Column> columns, Object[] values, int startIndex, JdbcConnection connection)
            throws SQLException {
        int paramIndex = startIndex;
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j <= i; j++) {
                if (values[j] != null) {
                    connection.setQueryColumnValue(statement, columns.get(j), paramIndex++, values[j]);
                }
            }
        }
        return paramIndex;
    }
}