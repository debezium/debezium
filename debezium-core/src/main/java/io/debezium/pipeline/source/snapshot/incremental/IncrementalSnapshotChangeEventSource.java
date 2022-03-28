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
public interface IncrementalSnapshotChangeEventSource<P extends Partition, T extends DataCollectionId> {

    void closeWindow(P partition, String id, OffsetContext offsetContext) throws InterruptedException;

    void processMessage(P partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException;

    void init(P partition, OffsetContext offsetContext);

    void addDataCollectionNamesToSnapshot(P partition, List<String> dataCollectionIds, OffsetContext offsetContext)
            throws InterruptedException;

    default void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processFilteredEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processTransactionStartedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processTransactionCommittedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
    }

    default void processSchemaChange(P partition, DataCollectionId dataCollectionId) throws InterruptedException {
    }
}
