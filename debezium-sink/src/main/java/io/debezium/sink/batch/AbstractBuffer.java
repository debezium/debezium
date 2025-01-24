/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.LinkedHashMap;
import java.util.List;

import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.util.LinkedHashMapExtractor;

public abstract class AbstractBuffer implements Buffer {

    protected final LinkedHashMap<Object, DebeziumSinkRecord> records = new LinkedHashMap<>();
    protected final SinkConnectorConfig connectorConfig;
    private final int batchSize;

    public AbstractBuffer(SinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.batchSize = connectorConfig.getBatchSize();
    }

    @Override
    public int size() {
        synchronized (records) {
            return records.size();
        }
    }

    @Override
    public void truncate(DebeziumSinkRecord record) {
        // clear all batches for the resolved collection/table
        records.clear();
        // add the event to the batch
        enqueue(record);
    }

    public void enqueue(DebeziumSinkRecord record) {
        records.put(record.key(), record);
    }

    @Override
    public DebeziumSinkRecord remove(DebeziumSinkRecord record) {
        return records.remove(record.key());
    }

    @Override
    public Batch poll() {
        if (batchSize <= records.size()) {
            return new Batch(LinkedHashMapExtractor.extractFirstEntries(records, batchSize));
        }
        return new Batch(List.of());
    }

}
