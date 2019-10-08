/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.relational.TableId;

/**
 * A class invoked by {@link SnapshotChangeEventSource} whenever an important event or change of state happens.
 *
 * @author Jiri Pechanec
 */
public interface SnapshotProgressListener {

    void snapshotStarted();

    void monitoredTablesDetermined(Iterable<TableId> tableIds);

    void snapshotCompleted();

    void snapshotAborted();

    void tableSnapshotCompleted(TableId tableId, long numRows);

    void rowsScanned(TableId tableId, long numRows);

    public static SnapshotProgressListener NO_OP = new SnapshotProgressListener() {

        @Override
        public void snapshotStarted() {
        }

        @Override
        public void rowsScanned(TableId tableId, long numRows) {
        }

        @Override
        public void monitoredTablesDetermined(Iterable<TableId> tableIds) {
        }

        @Override
        public void tableSnapshotCompleted(TableId tableId, long numRows) {
        }

        @Override
        public void snapshotCompleted() {
        }

        @Override
        public void snapshotAborted() {
        }
    };
}
