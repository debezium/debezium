/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * A passthrough buffer for keyless {@link DebeziumSinkRecord}s that preserves all events
 * using a sequence counter as the internal key.
 *
 * @author rk3rn3r
 */
public class KeylessPassthroughBuffer extends AbstractBuffer implements Buffer {

    private long sequence = 0;

    public KeylessPassthroughBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        records.put(new BatchKey(collectionId, sequence++), new BatchRecord(collectionId, record));
    }
}
