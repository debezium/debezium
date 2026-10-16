/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.DebeziumEngine;

/**
 * {@link DebeziumEngine.RecordCommitter} handed to a batch handler, which also marks as processed the records of the batch that were
 * dropped by a transformation and therefore never reach the handler. Without it, the offset of a dropped record is never recorded, so
 * a batch in which every record is dropped (for example heartbeats filtered out by a predicate) never advances the committed offset.
 * <p>
 * A dropped record is marked right before the next record of the batch that the handler marks, or when the handler finishes the batch
 * if the handler has marked all the records before it, so the recorded offset never gets ahead of a record that the handler has not
 * marked yet.
 * The implementation is not thread safe and is meant to be used for a single batch.
 */
class DroppedRecordsCommitter<R> implements DebeziumEngine.RecordCommitter<R> {

    private final DebeziumEngine.RecordCommitter<R> delegate;
    private final DebeziumEngine.RecordCommitter<SourceRecord> sourceCommitter;
    private final List<SourceRecord> sourceRecords;
    private final List<R> processedRecords;
    private final Map<R, Integer> positions;
    private int nextPosition = 0;

    /**
     * @param delegate the committer of the records passed to the handler
     * @param sourceCommitter the committer used to mark the dropped records, sharing the offset state with {@code delegate}
     * @param sourceRecords the records of the batch, in source order
     * @param processedRecords the result of the processing of each source record, at the same position; {@code null} for a dropped record
     */
    DroppedRecordsCommitter(DebeziumEngine.RecordCommitter<R> delegate, DebeziumEngine.RecordCommitter<SourceRecord> sourceCommitter,
                            List<SourceRecord> sourceRecords, List<R> processedRecords) {
        this.delegate = delegate;
        this.sourceCommitter = sourceCommitter;
        this.sourceRecords = sourceRecords;
        this.processedRecords = processedRecords;
        this.positions = new IdentityHashMap<>(processedRecords.size());
        for (int i = 0; i < processedRecords.size(); i++) {
            if (processedRecords.get(i) != null) {
                positions.put(processedRecords.get(i), i);
            }
        }
    }

    @Override
    public void markProcessed(R record) throws InterruptedException {
        markDroppedBefore(record);
        delegate.markProcessed(record);
    }

    @Override
    public void markProcessed(R record, DebeziumEngine.Offsets sourceOffsets) throws InterruptedException {
        markDroppedBefore(record);
        delegate.markProcessed(record, sourceOffsets);
    }

    @Override
    public void markBatchFinished() throws InterruptedException {
        // The handler may finish the batch without marking all of its records (e.g. when the engine is stopped),
        // so only the dropped records up to the first record the handler has not marked are marked
        for (; nextPosition < sourceRecords.size() && processedRecords.get(nextPosition) == null; nextPosition++) {
            sourceCommitter.markProcessed(sourceRecords.get(nextPosition));
        }
        delegate.markBatchFinished();
    }

    @Override
    public DebeziumEngine.Offsets buildOffsets() {
        return delegate.buildOffsets();
    }

    private void markDroppedBefore(R record) throws InterruptedException {
        final Integer position = positions.get(record);
        if (position != null) {
            markDroppedUpTo(position);
            nextPosition = Math.max(nextPosition, position + 1);
        }
    }

    private void markDroppedUpTo(int position) throws InterruptedException {
        for (; nextPosition < position; nextPosition++) {
            if (processedRecords.get(nextPosition) == null) {
                sourceCommitter.markProcessed(sourceRecords.get(nextPosition));
            }
        }
    }
}
