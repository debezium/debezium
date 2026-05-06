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
 * A buffer that fills a batch that deduplicates {@link DebeziumSinkRecord}s based on primary key.
 * It requires the key set to a unique value for the table.
 *
 * @author rk3rn3r
 */
public class DeduplicatingBuffer extends AbstractKeyedBuffer implements Buffer {

    public DeduplicatingBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    protected void doEnqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        records.put(new BatchKey(collectionId, record.key()), new BatchRecord(collectionId, record));
    }
}
