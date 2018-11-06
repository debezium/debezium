/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Set;

import io.debezium.relational.TableId;

/**
 * A class invoked by {@link SnapshotChangeEventSource} whenever an important event or change of state happens.
 *
 * @author Jiri Pechanec
 *
 */
public interface SnapshotProgressListener {

    public void completeTable(TableId tableId, long numRows);

    public void startSnapshot();

    public void completeSnapshot();

    public void abortSnapshot();

    public void setRowsScanned(TableId tableId, long numRows);

    public void setMonitoredTables(Set<TableId> tableIds);

    public static SnapshotProgressListener NO_OP = new SnapshotProgressListener() {

        @Override
        public void startSnapshot() {
        }

        @Override
        public void setRowsScanned(TableId tableId, long numRows) {

        }

        @Override
        public void setMonitoredTables(Set<TableId> tableIds) {

        }

        @Override
        public void completeTable(TableId tableId, long numRows) {

        }

        @Override
        public void completeSnapshot() {
        }

        @Override
        public void abortSnapshot() {
        }
    };
}
