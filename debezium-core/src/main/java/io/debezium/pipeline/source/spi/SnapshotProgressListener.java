/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import static io.debezium.util.Iterators.toIterable;
import static io.debezium.util.Iterators.transform;

import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * A class invoked by {@link SnapshotChangeEventSource} whenever an important event or change of state happens.
 *
 * @author Jiri Pechanec
 */
public interface SnapshotProgressListener {

    void snapshotStarted();

    /**
     * @deprecated Since 1.1, use {@link #monitoredCollectionsDetermined(Iterable)} instead.
     */
    @Deprecated
    default void monitoredTablesDetermined(Iterable<TableId> tableIds) {
        monitoredCollectionsDetermined(toIterable(transform(tableIds.iterator(), tableId -> tableId)));
    }

    void monitoredCollectionsDetermined(Iterable<DataCollectionId> dataCollectionIds);

    void snapshotCompleted();

    void snapshotAborted();

    /**
     * @deprecated Since 1.1, use {@link #dataCollectionSnapshotCompleted(DataCollectionId, long)} instead.
     */
    @Deprecated
    default void tableSnapshotCompleted(TableId id, long numRows) {
        dataCollectionSnapshotCompleted(id, numRows);
    }

    void dataCollectionSnapshotCompleted(DataCollectionId dataCollectionId, long numRows);

    void rowsScanned(TableId tableId, long numRows);

    public static SnapshotProgressListener NO_OP = new SnapshotProgressListener() {

        @Override
        public void snapshotStarted() {
        }

        @Override
        public void rowsScanned(TableId tableId, long numRows) {
        }

        @Override
        public void monitoredCollectionsDetermined(Iterable<DataCollectionId> dataCollectionIds) {
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
    };
}
