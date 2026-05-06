/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.ArrayList;
import java.util.List;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * A passthrough buffer for keyed {@link DebeziumSinkRecord}s that preserves all events
 * without deduplication. When a duplicate key is detected, the current buffer contents
 * are flushed into completed batches before the new record is added.
 * Each completed batch is guaranteed to contain no duplicate keys.
 *
 * @author rk3rn3r
 */
public class KeyedPassthroughBuffer extends AbstractKeyedBuffer implements Buffer {

    private final List<Batch> completedBatches = new ArrayList<>();

    public KeyedPassthroughBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected void doEnqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        BatchKey key = new BatchKey(collectionId, record.key());
        if (records.containsKey(key)) {
            completedBatches.addAll(splitByBatchSize(records));
            records.clear();
        }
        records.put(key, new BatchRecord(collectionId, record));
    }

    @Override
    public List<Batch> poll() {
        List<Batch> result = new ArrayList<>(completedBatches);
        completedBatches.clear();
        result.addAll(super.poll());
        return result;
    }

    @Override
    public List<Batch> forcePoll() {
        List<Batch> result = new ArrayList<>(completedBatches);
        completedBatches.clear();
        result.addAll(super.forcePoll());
        return result;
    }

    @Override
    public void truncate(CollectionId collectionId, DebeziumSinkRecord record) {
        completedBatches.replaceAll(batch -> {
            List<BatchRecord> filtered = batch.stream()
                    .filter(br -> !br.collectionId().equals(collectionId))
                    .toList();
            return new Batch(filtered);
        });
        completedBatches.removeIf(Batch::isEmpty);
        super.truncate(collectionId, record);
    }
}
