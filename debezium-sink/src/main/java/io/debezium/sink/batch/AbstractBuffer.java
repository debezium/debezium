/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.LinkedHashMap;
import java.util.List;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.util.LinkedHashMapExtractor;

public abstract class AbstractBuffer implements Buffer {

    protected final LinkedHashMap<BatchKey, BatchRecord> records = new LinkedHashMap<>();
    protected final SinkConnectorConfig connectorConfig;
    private final int batchSize;

    public AbstractBuffer(SinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.batchSize = connectorConfig.getBatchSize();
    }

    protected record BatchKey(CollectionId collectionId, Object key) {
    }

    @Override
    public int size() {
        synchronized (records) {
            return records.size();
        }
    }

    @Override
    public void truncate(CollectionId collectionId, DebeziumSinkRecord record) {
        // clear all entries with the same collectionId
        records.keySet().removeIf(batchKey -> batchKey.collectionId().equals(collectionId));
        // add the truncate event to the batches for the resolved collection/table
        enqueue(collectionId, record);
    }

    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        records.put(new BatchKey(collectionId, record.key()), new BatchRecord(collectionId, record));
    }

    @Override
    public Batch poll() {
        if (batchSize <= records.size()) {
            return new Batch(LinkedHashMapExtractor.extractFirstEntries(records, batchSize));
        }
        return new Batch(List.of());
    }

    public Batch forcePoll() {
        if (!records.isEmpty()) {
            return new Batch(LinkedHashMapExtractor.extractFirstEntries(records, records.size()));
        }
        return new Batch(List.of());
    }

}
