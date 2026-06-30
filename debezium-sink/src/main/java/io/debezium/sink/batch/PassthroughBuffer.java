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
 * A passthrough buffer that preserves all {@link DebeziumSinkRecord}s without deduplication.
 * <p>
 * In keyed mode, duplicate keys trigger a batch split — the current buffer contents are
 * flushed into completed batches before the new record is added. Each batch is guaranteed
 * to contain no duplicate keys.
 * <p>
 * In keyless mode, a sequence counter ensures every record gets a unique internal key.
 * <p>
 * Truncate events flush all pending records before the truncate is enqueued, ensuring
 * pre-truncate events are written to the downstream datastore before the table is truncated.
 *
 * @author rk3rn3r
 */
public class PassthroughBuffer extends AbstractBuffer implements Buffer {

    private final boolean keyed;
    private final List<Batch> completedBatches = new ArrayList<>();
    private long sequence = 0;

    public PassthroughBuffer(SinkConnectorConfig connectorConfig, boolean keyed) {
        super(connectorConfig);
        this.keyed = keyed;
    }

    @Override
    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        if (keyed) {
            BatchKey key = new BatchKey(
                    collectionId,
                    record.getFilteredKey(connectorConfig.getPrimaryKeyMode(), connectorConfig.getPrimaryKeyFields(), connectorConfig.fieldFilter()));

            if (records.containsKey(key)) {
                completedBatches.addAll(splitByBatchSize(records, true));
            }
            records.put(key, new BatchRecord(collectionId, record));
        }
        else {
            records.put(new BatchKey(collectionId, sequence++), new BatchRecord(collectionId, record));
        }
    }

    @Override
    public void truncate(CollectionId collectionId, DebeziumSinkRecord record) {
        if (!records.isEmpty()) {
            /*
             * For passthrough includeRemainder must be true because truncate events can only be
             * the first event in a batch to ensure overall ordering. Furthermore, all events in
             * the buffer must be flush before truncate events can be applied.
             */
            completedBatches.addAll(splitByBatchSize(records, true));
        }
        records.put(new BatchKey(collectionId, record.key()), new BatchRecord(collectionId, record));
    }

    @Override
    public List<Batch> poll() {
        if (!completedBatches.isEmpty()) {
            List<Batch> result = new ArrayList<>(completedBatches);
            completedBatches.clear();
            result.addAll(splitByBatchSize(records, false));
            return result;
        }
        return super.poll();
    }

    @Override
    public List<Batch> forcePoll() {
        List<Batch> result = new ArrayList<>(completedBatches);
        completedBatches.clear();
        if (!records.isEmpty()) {
            result.addAll(splitByBatchSize(records, true));
        }
        return result;
    }
}
