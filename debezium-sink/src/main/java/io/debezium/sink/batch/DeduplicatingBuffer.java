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
 * A buffer that deduplicates {@link DebeziumSinkRecord}s for optimal downstream writes.
 * <p>
 * In keyed mode, records with the same key overwrite previous entries (last-write-wins).
 * <p>
 * In keyless mode, a sequence counter ensures every record gets a unique internal key
 * (no deduplication of regular records is possible without keys).
 * <p>
 * Truncate events remove all pending records for the collection from the buffer
 * before the truncate is enqueued — an optimization since the truncate will clear the
 * downstream table anyway.
 *
 * @author rk3rn3r
 */
public class DeduplicatingBuffer extends AbstractBuffer implements Buffer {

    private final boolean keyed;
    private long sequence = 0;

    public DeduplicatingBuffer(SinkConnectorConfig connectorConfig, boolean keyed) {
        super(connectorConfig);
        this.keyed = keyed;
    }

    @Override
    public void truncate(CollectionId collectionId, DebeziumSinkRecord record) {
        // clear all entries with the same collectionId
        records.keySet().removeIf(batchKey -> batchKey.collectionId().equals(collectionId));
        records.put(new BatchKey(collectionId, record.key()), new BatchRecord(collectionId, record));
    }

    @Override
    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        if (keyed) {
            records.put(
                    new BatchKey(collectionId,
                            record.getFilteredKey(connectorConfig.getPrimaryKeyMode(), connectorConfig.getPrimaryKeyFields(), connectorConfig.fieldFilter())),
                    new BatchRecord(collectionId, record));
        }
        else {
            records.put(new BatchKey(collectionId, sequence++), new BatchRecord(collectionId, record));
        }
    }

}
