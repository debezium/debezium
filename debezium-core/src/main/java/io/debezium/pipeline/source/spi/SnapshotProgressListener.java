/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * Invoked whenever an important event or change of state happens during the snapshot phase.
 *
 * @author Jiri Pechanec
 */
public interface SnapshotProgressListener {

    void snapshotStarted();

    void monitoredDataCollectionsDetermined(Iterable<? extends DataCollectionId> dataCollectionIds);

    void snapshotCompleted();

    void snapshotAborted();

    void dataCollectionSnapshotCompleted(DataCollectionId dataCollectionId, long numRows);

    void rowsScanned(TableId tableId, long numRows);

    void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo);

    void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo, Object tableTo[]);

    public static SnapshotProgressListener NO_OP = new SnapshotProgressListener() {

        @Override
        public void snapshotStarted() {
        }

        @Override
        public void rowsScanned(TableId tableId, long numRows) {
        }

        @Override
        public void monitoredDataCollectionsDetermined(Iterable<? extends DataCollectionId> dataCollectionIds) {
        }

        @Override
        public void dataCollectionSnapshotCompleted(DataCollectionId dataCollectionId, long numRows) {
        }

        @Override
        public void snapshotCompleted() {
        }

        @Override
        public void snapshotAborted() {
        }

        @Override
        public void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        }

        @Override
        public void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo, Object tableTo[]) {
        }
    };
}
