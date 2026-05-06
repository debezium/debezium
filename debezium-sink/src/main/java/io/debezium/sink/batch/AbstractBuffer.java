/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import io.debezium.metadata.CollectionId;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.util.BufferExtractor;

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
        return records.size();
    }

    protected int getBatchSize() {
        return batchSize;
    }

    protected List<Batch> splitByBatchSize(LinkedHashMap<BatchKey, BatchRecord> map, boolean includeRemainder) {
        List<Batch> batches = new ArrayList<>();
        while (includeRemainder ? !map.isEmpty() : map.size() >= batchSize) {
            int count = Math.min(batchSize, map.size());
            batches.add(new Batch(BufferExtractor.extractFirstEntries(map, count)));
        }
        return batches;
    }

    @Override
    public List<Batch> poll() {
        int batchSize = getBatchSize();
        if (batchSize <= records.size()) {
            return Collections.singletonList(new Batch(BufferExtractor.extractFirstEntries(records, batchSize)));
        }
        return List.of();
    }

    @Override
    public List<Batch> forcePoll() {
        if (!records.isEmpty()) {
            return Collections.singletonList(new Batch(BufferExtractor.extractFirstEntries(records, records.size())));
        }
        return List.of();
    }

}
