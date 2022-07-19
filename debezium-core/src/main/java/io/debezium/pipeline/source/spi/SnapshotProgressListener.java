/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Invoked whenever an important event or change of state happens during the snapshot phase.
 *
 * @author Jiri Pechanec
 */
public interface SnapshotProgressListener<P extends Partition> {

    void snapshotStarted(P partition);

    void snapshotPaused(P partition);

    void snapshotResumed(P partition);

    void monitoredDataCollectionsDetermined(P partition, Iterable<? extends DataCollectionId> dataCollectionIds);

    void snapshotCompleted(P partition);

    void snapshotAborted(P partition);

    void dataCollectionSnapshotCompleted(P partition, DataCollectionId dataCollectionId, long numRows);

    void rowsScanned(P partition, TableId tableId, long numRows);

    void currentChunk(P partition, String chunkId, Object[] chunkFrom, Object[] chunkTo);

    void currentChunk(P partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo);

    static <P extends Partition> SnapshotProgressListener<P> NO_OP() {
        return new SnapshotProgressListener<P>() {

            @Override
            public void snapshotStarted(P partition) {
            }

            @Override
            public void snapshotPaused(P partition) {
            }

            @Override
            public void snapshotResumed(P partition) {
            }

            @Override
            public void rowsScanned(P partition, TableId tableId, long numRows) {
            }

            @Override
            public void monitoredDataCollectionsDetermined(P partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
            }

            @Override
            public void dataCollectionSnapshotCompleted(P partition, DataCollectionId dataCollectionId, long numRows) {
            }

            @Override
            public void snapshotCompleted(P partition) {
            }

            @Override
            public void snapshotAborted(P partition) {
            }

            @Override
            public void currentChunk(P partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
            }

            @Override
            public void currentChunk(P partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
            }
        };
    }
}
