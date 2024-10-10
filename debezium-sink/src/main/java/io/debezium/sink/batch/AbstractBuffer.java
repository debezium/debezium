/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

public abstract class AbstractBuffer implements Buffer {

    protected final Map<CollectionId, LinkedHashMap<Object, DebeziumSinkRecord>> records = new LinkedHashMap<>();
    protected final SinkConnectorConfig connectorConfig;
    private final int batchSize;

    public AbstractBuffer(SinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.batchSize = connectorConfig.getBatchSize();
    }

    @Override
    public int size() {
        synchronized (records) {
            return records.values().stream()
                    .mapToInt(Map::size)
                    .sum();
        }
    }

    @Override
    public void truncate(CollectionId collectionId, DebeziumSinkRecord record) {
        // clear all batches for the resolved collection/table
        records.remove(collectionId);
        // add the event to the batch
        enqueue(collectionId, record);
    }

    @Override
    public Batch poll() {
        return new Batch(records.entrySet().stream().limit(batchSize).collect(Collectors.toSet()));
    }

}
