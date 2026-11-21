/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Reads one chunk of the current data collection per window on the streaming connection and buffers its rows in a
 * single window. The chunk itself is read by {@link AbstractIncrementalSnapshotChangeEventSource#readChunk}; this
 * strategy only owns the window.
 */
public class SequentialIncrementalSnapshotReadStrategy<P extends Partition, T extends DataCollectionId>
        implements IncrementalSnapshotReadStrategy<P, T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequentialIncrementalSnapshotReadStrategy.class);

    private final Map<Struct, Object[]> window;
    private final WindowSink<P, T> sink;

    public SequentialIncrementalSnapshotReadStrategy(Map<Struct, Object[]> window, WindowSink<P, T> sink) {
        this.window = window;
        this.sink = sink;
    }

    @Override
    public Map<Struct, Object[]> windowFor(T dataCollectionId) {
        return window;
    }

    @Override
    public boolean hasBufferedEvents() {
        return !window.isEmpty();
    }

    @Override
    public boolean buffersEventsFor(IncrementalSnapshotContext<T> context, DataCollectionId dataCollectionId) {
        return context.currentDataCollectionId() != null
                && context.currentDataCollectionId().getId().equals(dataCollectionId);
    }

    @Override
    public void drainWindows(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) throws InterruptedException {
        // No currentDataCollectionId() deref outside the loop: it can be null on a window close after the last collection.
        LOGGER.debug("Sending {} events from window buffer", window.size());
        offsetContext.incrementalSnapshotEvents();
        for (Object[] row : window.values()) {
            sink.send(partition, offsetContext, context.currentDataCollectionId().getId(), row);
        }
        offsetContext.postSnapshotCompletion();
        window.clear();
    }

    @Override
    public boolean readRound(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) {
        return false;
    }

    @Override
    public void tablesAdded(List<DataCollection<T>> tables) {
    }

    @Override
    public void tableRemoved(T tableId) {
    }

    @Override
    public boolean tableSchemaChanged(T tableId) {
        return false;
    }

    @Override
    public void snapshotCompleted() {
    }

    @Override
    public void close() {
    }
}
