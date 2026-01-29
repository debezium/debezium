/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.chunked;

import java.util.OptionalLong;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Represents a single chunk of a table to be snapshot in parallel with precomputed values.
 *
 * @author Chris Cranford
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SnapshotChunk {

    private final TableId tableId;
    private final Table table;
    private final Object[] lowerBounds;
    private final Object[] upperBounds;
    private final int chunkIndex;
    private final int totalChunks;
    private final int tableOrder;
    private final int tableCount;
    private final String baseSelectStatement;
    private final OptionalLong estimatedRowCount;

    public SnapshotChunk(TableId tableId, Table table, Object[] lowerBounds, Object[] upperBounds, int chunkIndex,
                         int totalChunks, int tableOrder, int tableCount, String baseSelectStatement, OptionalLong estimatedRowCount) {
        this.tableId = tableId;
        this.table = table;
        this.lowerBounds = lowerBounds;
        this.upperBounds = upperBounds;
        this.chunkIndex = chunkIndex;
        this.totalChunks = totalChunks;
        this.tableOrder = tableOrder;
        this.tableCount = tableCount;
        this.baseSelectStatement = baseSelectStatement;
        this.estimatedRowCount = estimatedRowCount;
    }

    public TableId getTableId() {
        return tableId;
    }

    public Table getTable() {
        return table;
    }

    public Object[] getLowerBounds() {
        return lowerBounds;
    }

    public Object[] getUpperBounds() {
        return upperBounds;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public int getTotalChunks() {
        return totalChunks;
    }

    public int getTableOrder() {
        return tableOrder;
    }

    public int getTableCount() {
        return tableCount;
    }

    public String getBaseSelectStatement() {
        return baseSelectStatement;
    }

    public OptionalLong getEstimatedRowCount() {
        return estimatedRowCount;
    }

    public boolean hasLowerBound() {
        return lowerBounds != null;
    }

    public boolean hasUpperBound() {
        return upperBounds != null;
    }

    public boolean isFirstChunk() {
        return chunkIndex == 0;
    }

    public boolean isLastChunk() {
        return chunkIndex == totalChunks - 1;
    }

    public boolean isFirstChunkOfSnapshot() {
        return tableOrder == 1 && isFirstChunk();
    }

    public boolean isLastChunkOfSnapshot() {
        return tableOrder == tableCount && isLastChunk();
    }

    public String getChunkId() {
        return tableId.identifier() + "_chunk_" + chunkIndex;
    }
}
