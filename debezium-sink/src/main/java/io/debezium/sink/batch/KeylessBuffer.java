/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.LinkedHashMap;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * A buffer of {@link DebeziumSinkRecord}. It contains the logic of when is the time to flush
 *
 * @author rk3rn3r
 */
public class KeylessBuffer extends AbstractBuffer implements Buffer {

    public KeylessBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        records.computeIfAbsent(collectionId, k -> new LinkedHashMap<>());
        records.get(collectionId).computeIfAbsent(record.key(), k -> record);
    }

    @Override
    public DebeziumSinkRecord remove(CollectionId collectionId, DebeziumSinkRecord record) {
        return records.get(collectionId).remove(record.key());
    }
}
