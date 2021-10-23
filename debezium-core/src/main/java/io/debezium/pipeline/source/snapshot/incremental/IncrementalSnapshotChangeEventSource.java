/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DataCollectionId;

/**
 * A Contract t
 * 
 * @author Jiri Pechanec
 *
 * @param <T> data collection id class
 */
public interface IncrementalSnapshotChangeEventSource<T extends DataCollectionId> {

    void closeWindow(Partition partition, String id, OffsetContext offsetContext) throws InterruptedException;

    void processMessage(Partition partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException;

    void init(OffsetContext offsetContext);

    void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, OffsetContext offsetContext)
            throws InterruptedException;

    default void processHeartbeat(Partition partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processFilteredEvent(Partition partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processTransactionStartedEvent(Partition partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processTransactionCommittedEvent(Partition partition, OffsetContext offsetContext) throws InterruptedException {
    }
}
