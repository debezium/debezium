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

    T currentDataCollectionId();

    T nextDataCollection();

    List<T> addDataCollectionNamesToSnapshot(List<String> dataCollectionIds);

    int tablesToBeSnapshottedCount();

    boolean openWindow(String id);

    boolean closeWindow(String id);

    boolean isNonInitialChunk();

    boolean snapshotRunning();

    void startNewChunk();

    void nextChunkPosition(Object[] lastKey);

    String currentChunkId();

    Object[] chunkEndPosititon();

    void sendEvent(Object[] keyFromRow);

    void maximumKey(Object[] key);

    Optional<Object[]> maximumKey();

    boolean deduplicationNeeded();

    Map<String, Object> store(Map<String, Object> offset);

    void revertChunk();

    void setSchema(Table schema);

    Table getSchema();

    boolean isSchemaVerificationPassed();

    void setSchemaVerificationPassed(boolean schemaVerificationPassed);
}
