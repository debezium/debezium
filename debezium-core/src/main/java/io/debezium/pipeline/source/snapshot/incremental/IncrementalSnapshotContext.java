/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.relational.Table;

public interface IncrementalSnapshotContext<T> {

    DataCollection<T> nextDataCollection();

    List<DataCollection<T>> addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, Optional<String> additionalCondition, Optional<String> surrogateKey);

    int dataCollectionsToBeSnapshottedCount();

    boolean openWindow(String id, T dataCollectionId);

    boolean closeWindow(String id, T dataCollectionId);

    void pauseSnapshot();

    void resumeSnapshot();

    boolean isSnapshotPaused();

    boolean snapshotRunning();

    void sendEvent(Object[] keyFromRow, T dataCollectionId);

    Map<String, Object> store(Map<String, Object> offset);

    void stopSnapshot();

    boolean removeDataCollectionFromSnapshot(String dataCollectionId);
}
