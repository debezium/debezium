/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Calculates chunk boundaries for chunked table snapshots.
 *
 * @author Chris Cranford
 */
public class ChunkBoundaryCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkBoundaryCalculator.class);

    private final JdbcConnection jdbcConnection;
    private final ChunkQueryBuilder<DataCollectionId> connectionChunkQueryBuilder;

    public ChunkBoundaryCalculator(JdbcConnection jdbcConnection, RelationalDatabaseConnectorConfig config) {
        this.jdbcConnection = jdbcConnection;
        this.connectionChunkQueryBuilder = jdbcConnection.chunkQueryBuilder(config);

    }

    /**
     * Calculate chunk boundaries for a table.
     *
     * @param table The table to chunk
     * @param keyColumns The columns to use for chunking (PK or message.key.columns)
     * @param rowCount Estimated row count
     * @param numChunks Desired number of chunks
     * @return List of boundary value arrays (each array has values for all key columns)
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public List<Object[]> calculateBoundaries(Table table, List<Column> keyColumns, OptionalLong rowCount, int numChunks) throws SQLException {
        if (keyColumns.isEmpty() || numChunks <= 1) {
            return List.of(); // No boundaries needed for single chunk
        }

        final TableId tableId = table.id();
        final List<Object[]> boundaries = new ArrayList<>();

        if (rowCount.isEmpty() || rowCount.getAsLong() == 0) {
            LOGGER.debug("Unknown or zero row count for table {}, using single chunk", tableId);
            return boundaries;
        }

        final long count = rowCount.getAsLong();
        final long chunkSize = count / numChunks;
        LOGGER.debug("Chunk boundaries based on {} count with chunk size {}.", count, chunkSize);

        if (chunkSize == 0) {
            LOGGER.debug("Chunk size would be 0 for table {}, using single chunk", tableId);
            return boundaries;
        }

        Object[] previousBoundaryValue = null;
        for (int i = 1; i < numChunks; i++) {
            LOGGER.debug("Querying Boundary at position {} for chunk #{}", i * chunkSize, i);
            final String sql = buildNextBoundaryQuery(tableId, keyColumns, chunkSize, previousBoundaryValue);
            final Object[] boundaryValue = queryColumnValues(keyColumns, sql, previousBoundaryValue);
            if (boundaryValue != null) {
                boundaries.add(boundaryValue);
                previousBoundaryValue = boundaryValue;
            }
        }

        LOGGER.debug("Calculated {} boundaries for table {} ({} chunks)", boundaries.size(), tableId, boundaries.size() + 1);
        return boundaries;
    }

    public String buildNextBoundaryQuery(TableId tableId, List<Column> keyColumns, long chunkSize, Object[] lowerBoundBoundary) {
        String lowerBoundBoundaryCondition = null;
        if (lowerBoundBoundary != null) {
            StringBuilder lowerBoundBoundaryConditionBuilder = new StringBuilder();
            connectionChunkQueryBuilder.addLowerBound(keyColumns, lowerBoundBoundary, lowerBoundBoundaryConditionBuilder, true);
            lowerBoundBoundaryCondition = lowerBoundBoundaryConditionBuilder.toString();
        }
        final String keyColumnNames = String.join(", ", keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .toList());
        final String sql = jdbcConnection.buildSelectPrimaryKeyBoundaries(tableId, chunkSize, keyColumnNames, keyColumnNames, lowerBoundBoundaryCondition);

        LOGGER.debug("Boundary query: {}", sql);
        return sql;
    }

    /**
     * Create SnapshotChunk objects from calculated boundaries.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public List<SnapshotChunk> createChunks(Table table, List<Object[]> boundaries, int tableOrder, int tableCount, String baseSelectStatement,
                                            OptionalLong totalRowCount, Object[] maximumKey) {
        final List<SnapshotChunk> chunks = new ArrayList<>();
        final int numChunks = boundaries.size() + 1;
        final OptionalLong chunkRowEstimate = totalRowCount.isPresent()
                ? OptionalLong.of(totalRowCount.getAsLong() / numChunks)
                : OptionalLong.empty();

        for (int i = 0; i < numChunks; i++) {
            final Object[] lowerBound = (i == 0) ? null : boundaries.get(i - 1);
            final Object[] upperBound = (i == numChunks - 1) ? maximumKey : boundaries.get(i);

            chunks.add(new SnapshotChunk(
                    table.id(),
                    table,
                    lowerBound,
                    upperBound,
                    i,
                    numChunks,
                    tableOrder,
                    tableCount,
                    baseSelectStatement,
                    chunkRowEstimate));
        }

        return chunks;
    }

    public Object[] calculateMaxKey(Table table, List<Column> keyColumns) throws SQLException {
        final String projection = String.join(", ", keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .toList());

        final String orderBy = keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .collect(Collectors.joining(" DESC, ")) + " DESC";

        final String sql = jdbcConnection.buildSelectWithRowLimits(table.id(), 1, projection, Optional.empty(), Optional.empty(), orderBy);
        LOGGER.debug("Max key query for table {}: {}", table.id(), sql);

        return queryColumnValues(keyColumns, sql);
    }

    private Object[] queryColumnValues(List<Column> columns, String sql) throws SQLException {
        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                final Object[] values = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    values[i] = rs.getObject(i + 1);
                }
                return values;
            }
            return null;
        });
    }

    private Object[] queryColumnValues(List<Column> columns, String sql, Object[] lowerBoundBoundary) throws SQLException {
        try (PreparedStatement statement = jdbcConnection.connection().prepareStatement(sql)) {
            if (lowerBoundBoundary != null) {
                connectionChunkQueryBuilder.bindBoundaryParams(statement, columns, lowerBoundBoundary, 1, jdbcConnection);
            }
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    final Object[] values = new Object[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        values[i] = rs.getObject(i + 1);
                    }
                    return values;
                }
                return null;
            }
        }
    }
}
