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
 * A buffer of {@link DebeziumSinkRecord}
 *
 * @author rk3rn3r
 */
public class SimpleBuffer extends AbstractBuffer implements Buffer {

    private long sequence = 0;

    public SimpleBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        records.put(new BatchKey(collectionId, sequence++), new BatchRecord(collectionId, record));
    }
}
