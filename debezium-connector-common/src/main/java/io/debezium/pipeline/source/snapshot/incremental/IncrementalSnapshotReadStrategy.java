/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * How an incremental snapshot reads its chunks and buffers the rows of an open watermark window.
 *
 * <p>Chosen once per change event source: {@link SequentialIncrementalSnapshotReadStrategy} unless the
 * source supports parallel reads and more than one snapshot thread is configured, in which case
 * {@link ParallelIncrementalSnapshotReadStrategy} reads one chunk per active table in every window.
 *
 * @param <P> the partition type
 * @param <T> the data collection identifier type
 */
public interface IncrementalSnapshotReadStrategy<P extends Partition, T extends DataCollectionId> extends AutoCloseable {

    /**
     * Returns the buffer holding the rows read for the given data collection in the current window.
     */
    Map<Struct, Object[]> windowFor(T dataCollectionId);

    /**
     * Whether any window buffer currently holds rows.
     */
    boolean hasBufferedEvents();

    /**
     * Whether a streaming event of the given data collection must be deduplicated against its window buffer.
     */
    boolean buffersEventsFor(IncrementalSnapshotContext<T> context, DataCollectionId dataCollectionId);

    /**
     * Dispatches and clears the rows buffered in the current window.
     */
    void drainWindows(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) throws InterruptedException;

    /**
     * Reads one round of chunks. Returns {@code false} when the caller must read the next chunk of the current
     * data collection on the streaming connection instead.
     */
    boolean readRound(IncrementalSnapshotContext<T> context, P partition, OffsetContext offsetContext) throws InterruptedException;

    void tablesAdded(List<DataCollection<T>> tables);

    void tableRemoved(T tableId);

    /**
     * Returns {@code true} when the schema change was handled by the strategy, {@code false} when the caller
     * must re-read the current chunk.
     */
    boolean tableSchemaChanged(T tableId);

    void snapshotCompleted();

    @Override
    void close();

    /**
     * Dispatches one buffered row of a data collection as a snapshot event.
     */
    @FunctionalInterface
    interface WindowSink<P extends Partition, T extends DataCollectionId> {
        void send(P partition, OffsetContext offsetContext, T dataCollectionId, Object[] row) throws InterruptedException;
    }

    /**
     * Ends a data collection in the snapshot context and closes the snapshot when it was the last one.
     */
    @FunctionalInterface
    interface CollectionCompletion<P extends Partition, T extends DataCollectionId> {
        void completed(T dataCollectionId, P partition, OffsetContext offsetContext);
    }

    /**
     * The watermark window a round of chunks is read in.
     */
    interface WatermarkWindow<P extends Partition> {
        void open(P partition, OffsetContext offsetContext) throws SQLException;

        void close(P partition, OffsetContext offsetContext) throws Exception;
    }
}
