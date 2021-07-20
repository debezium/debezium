/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * A Contract t
 * 
 * @author Jiri Pechanec
 *
 * @param <T> data collection id class
 */
public interface IncrementalSnapshotChangeEventSource<T extends DataCollectionId> {

    void closeWindow(String id, OffsetContext offsetContext) throws InterruptedException;

    void processMessage(DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException;

    void init(OffsetContext offsetContext);

    void addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, OffsetContext offsetContext)
            throws InterruptedException;
}