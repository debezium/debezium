/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Calculates chunk boundaries for chunked table snapshots.
 *
 * @author Chris Cranford
 */
public class ChunkBoundaryCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkBoundaryCalculator.class);

    private final JdbcConnection jdbcConnection;

    public ChunkBoundaryCalculator(JdbcConnection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
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

        final String keyColumnNames = String.join(", ", keyColumns.stream()
                .map(c -> jdbcConnection.quoteIdentifier(c.name()))
                .toList());

        for (int i = 1; i < numChunks; i++) {
            final long position = i * chunkSize;

            final Object[] boundaryValue = queryBoundaryAtPosition(tableId, keyColumnNames, keyColumns, position);
            if (boundaryValue != null) {
                boundaries.add(boundaryValue);
            }
        }

        LOGGER.debug("Calculated {} boundaries for table {} ({} chunks)", boundaries.size(), tableId, boundaries.size() + 1);
        return boundaries;
    }

    private Object[] queryBoundaryAtPosition(TableId tableId, String keyColumnNames, List<Column> keyColumns, long position) throws SQLException {
        final String sql = jdbcConnection.buildSelectPrimaryKeyBoundaries(tableId, position, keyColumnNames, keyColumnNames);

        LOGGER.debug("Boundary query at position {}: {}", position, sql);

        return jdbcConnection.queryAndMap(sql, rs -> {
            if (rs.next()) {
                final Object[] values = new Object[keyColumns.size()];
                for (int i = 0; i < keyColumns.size(); i++) {
                    values[i] = rs.getObject(i + 1);
                }
                return values;
            }
            return null;
        });
    }

    /**
     * Create SnapshotChunk objects from calculated boundaries.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public List<SnapshotChunk> createChunks(Table table, List<Object[]> boundaries, int tableOrder, int tableCount, String baseSelectStatement,
                                            OptionalLong totalRowCount) {
        final List<SnapshotChunk> chunks = new ArrayList<>();
        final int numChunks = boundaries.size() + 1;
        final OptionalLong chunkRowEstimate = totalRowCount.isPresent()
                ? OptionalLong.of(totalRowCount.getAsLong() / numChunks)
                : OptionalLong.empty();

        for (int i = 0; i < numChunks; i++) {
            final Object[] lowerBound = (i == 0) ? null : boundaries.get(i - 1);
            final Object[] upperBound = (i == numChunks - 1) ? null : boundaries.get(i);

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
}
