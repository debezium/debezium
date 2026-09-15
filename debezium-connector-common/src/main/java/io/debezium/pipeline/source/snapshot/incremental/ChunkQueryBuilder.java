/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Builds queries for reading incremental snapshot chunks from a table.
 */
public interface ChunkQueryBuilder<T extends DataCollectionId> {

    /**
     * Builds a query for reading the next incremental snapshot chunk from a table using the chunk size configured by the connector.
     */
    String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition);

    /**
     * Builds a query for reading the next incremental snapshot chunk from a table using the specified limit.
     */
    String buildChunkQuery(IncrementalSnapshotContext<T> context, Table table, int limit, Optional<String> additionalCondition);

    /**
     * Appends an optimized inclusive/exclusive lower bound condition for the given columns. It uses boundaryValues to check for NULLs if needed.
     */
    void addLowerBound(List<Column> pkColumns, Object[] boundaryValues, StringBuilder condition, boolean inclusiveFinal);

    /**
     * Appends an optimized inclusive/exclusive upper bound condition for the given columns. It uses boundaryValues to check for NULLs if needed.
     */
    void addUpperBound(List<Column> pkColumns, Object[] boundaryValues, StringBuilder condition, boolean inclusiveFinal);

    /**
     * A single boundary parameter: the column it binds to and the value bound to it.
     */
    record QueryParam(Column column, Object value) {
    }

    /**
     * Generates the ordered list of boundary parameters for the given columns and the corresponding values.
     */
    List<QueryParam> generateBoundaryParams(List<Column> columns, Object[] values);

    /**
     * Generates Boundary Params (typically using {@link #generateBoundaryParams(List, Object[])}) for given columns and the corresponding values.
     * Then, binds generated Boundary Params to {@code statement} starting at position {@code startIndex}.
     */
    int bindBoundaryParams(PreparedStatement statement, List<Column> columns, Object[] values, int startIndex, JdbcConnection connection) throws SQLException;

    /**
     * Prepares a statement for reading the next incremental snapshot chunk from a table using the SQL statement returned by buildChunkQuery.
     */
    PreparedStatement readTableChunkStatement(IncrementalSnapshotContext<T> context, Table table, String sql) throws SQLException;

    /**
     * Builds a query for reading the maximum primary key value from a table.
     */
    String buildMaxPrimaryKeyQuery(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition);

    /**
     * Returns a best-effort, constant-time estimate of the number of rows in the table using connector-provided
     * metadata (for example PostgreSQL {@code pg_class.reltuples}).
     * <p>
     * The estimate is only valid for the whole table, so it must only be used when no {@code additionalConditions}
     * row filter is present. The default implementation returns {@link OptionalLong#empty()} (no estimate source);
     * connectors override it to expose their metadata estimate.
     *
     * @return the estimated row count, or {@link OptionalLong#empty()} when no estimate source is available
     */
    default OptionalLong estimateRowCount(IncrementalSnapshotContext<T> context, Table table) {
        return OptionalLong.empty();
    }

    /**
     * Returns the exact number of rows the incremental snapshot will scan for the table, i.e. the rows whose key is
     * less than or equal to {@code maximumKey} (optionally further constrained by {@code additionalCondition}).
     * <p>
     * Bounding the count by {@code maximumKey} matches exactly what the snapshot reads (later inserts flow through
     * streaming), so derived progress cannot exceed 100% under concurrent inserts. This is a best-effort operation:
     * a failure resolves to {@link OptionalLong#empty()} rather than failing the snapshot.
     * <p>
     * The default implementation returns {@link OptionalLong#empty()} (no count available); connectors override it
     * (typically via {@link AbstractChunkQueryBuilder}) to expose the bounded count.
     *
     * @return the bounded exact row count, or {@link OptionalLong#empty()} when it could not be determined
     */
    default OptionalLong countRows(IncrementalSnapshotContext<T> context, Table table, Optional<String> additionalCondition, Object[] maximumKey) {
        return OptionalLong.empty();
    }

    /**
     * Returns the columns that are used for paginating the incremental snapshot chunks.
     */
    List<Column> getQueryColumns(IncrementalSnapshotContext<T> context, Table table);

    /**
     * Allows builders to adjust the {@link Table} metadata before it is used for chunking.
     * <p>
     * Default implementation is a no-op, returning the provided table instance.
     */
    default Table prepareTable(IncrementalSnapshotContext<T> context, Table table) {
        return table;
    }

    /**
     * Resolves the chunk-end position for the next chunk.
     * <p>
     * Defaults to {@code lastRowKey}. Implementations may return a separately computed boundary when chunk ordering differs from result ordering.
     *
     * @param context the current incremental snapshot context
     * @param table the table being snapshotted
     * @param lastRowKey the key from the last row of the current chunk
     * @return the key to use as the next chunk start position
     */
    default Object[] resolveChunkEndPosition(IncrementalSnapshotContext<T> context, Table table, Object[] lastRowKey) {
        return lastRowKey;
    }
}
