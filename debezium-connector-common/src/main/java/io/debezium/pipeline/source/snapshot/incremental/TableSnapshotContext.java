/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.relational.Table;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Isolated context for tracking incremental snapshot progress of a single table.
 * Used by worker threads processing tables in parallel.
 *
 * <p>This is a lightweight, thread-local context that allows workers to independently
 * track their progression through a table's chunks without conflicting with the global
 * IncrementalSnapshotContext used by the main thread.
 *
 * <p>Implements IncrementalSnapshotContext interface but only supports methods needed
 * for chunk reading. Other methods throw UnsupportedOperationException.
 *
 * @author Ivan Senyk
 * @param <T> the type of data collection identifier
 */
public class TableSnapshotContext<T extends DataCollectionId> implements IncrementalSnapshotContext<T> {

    private final DataCollection<T> dataCollection;
    private Object[] maximumKey;
    private Object[] chunkEndPosition;
    private int currentChunkId = 0;
    private boolean completed = false;
    private Table schema;

    public TableSnapshotContext(DataCollection<T> dataCollection) {
        this.dataCollection = dataCollection;
    }

    // === Implemented methods (required for ChunkQueryBuilder) ===

    @Override
    public DataCollection<T> currentDataCollectionId() {
        return dataCollection;
    }

    @Override
    public Optional<Object[]> maximumKey() {
        return Optional.ofNullable(maximumKey);
    }

    @Override
    public void maximumKey(Object[] maximumKey) {
        this.maximumKey = maximumKey;
    }

    @Override
    public Object[] chunkEndPosititon() {
        return chunkEndPosition;
    }

    @Override
    public void nextChunkPosition(Object[] position) {
        this.chunkEndPosition = position;
        this.currentChunkId++;
    }

    @Override
    public boolean isNonInitialChunk() {
        return currentChunkId > 0;
    }

    @Override
    public void startNewChunk() {
        // Called before reading a new chunk
    }

    @Override
    public String currentChunkId() {
        return String.valueOf(currentChunkId);
    }

    @Override
    public Table getSchema() {
        return schema;
    }

    @Override
    public void setSchema(Table schema) {
        this.schema = schema;
    }

    // === Custom methods ===

    public boolean isCompleted() {
        return completed;
    }

    public void markCompleted() {
        this.completed = true;
    }

    // === Unsupported methods (not needed for worker threads) ===

    @Override
    public DataCollection<T> nextDataCollection() {
        throw new UnsupportedOperationException("Worker context manages single table only");
    }

    @Override
    public List<DataCollection<T>> addDataCollectionNamesToSnapshot(String correlationId, List<String> dataCollectionIds,
                                                                    List<AdditionalCondition> additionalCondition, String surrogateKey) {
        throw new UnsupportedOperationException("Worker context manages single table only");
    }

    @Override
    public int dataCollectionsToBeSnapshottedCount() {
        return 1;
    }

    @Override
    public boolean openWindow(String id) {
        return true;
    }

    @Override
    public boolean closeWindow(String id) {
        return true;
    }

    @Override
    public void pauseSnapshot() {
        throw new UnsupportedOperationException("Worker context does not support pause/resume");
    }

    @Override
    public void resumeSnapshot() {
        throw new UnsupportedOperationException("Worker context does not support pause/resume");
    }

    @Override
    public boolean isSnapshotPaused() {
        return false;
    }

    @Override
    public boolean snapshotRunning() {
        return !completed;
    }

    @Override
    public void sendEvent(Object[] keyFromRow) {
        // No-op for worker context
    }

    @Override
    public boolean deduplicationNeeded() {
        return true;
    }

    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        throw new UnsupportedOperationException("Worker context does not persist to offset");
    }

    @Override
    public void revertChunk() {
        if (currentChunkId > 0) {
            currentChunkId--;
        }
    }

    @Override
    public boolean isSchemaVerificationPassed() {
        return true;
    }

    @Override
    public void setSchemaVerificationPassed(boolean schemaVerificationPassed) {
        // No-op
    }

    @Override
    public void requestSnapshotStop(List<String> dataCollectionIds) {
        this.completed = true;
    }

    @Override
    public boolean removeDataCollectionFromSnapshot(String dataCollectionId) {
        return false;
    }

    @Override
    public List<DataCollection<T>> getDataCollections() {
        return List.of(dataCollection);
    }

    @Override
    public void unsetCorrelationId() {
        // No-op
    }

    @Override
    public String getCorrelationId() {
        return null;
    }

    @Override
    public List<String> getDataCollectionsToStop() {
        return List.of();
    }
}
