/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.connector.common.TaskPartition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * A class invoked by {@link SnapshotChangeEventSource} whenever an important event or change of state happens.
 *
 * @author Jiri Pechanec
 */
public interface SnapshotProgressListener<P extends TaskPartition> {

    void snapshotStarted(P partition);

    void monitoredDataCollectionsDetermined(P partition, Iterable<? extends DataCollectionId> dataCollectionIds);

    void snapshotCompleted(P partition);

    void snapshotAborted(P partition);

    void dataCollectionSnapshotCompleted(P partition, DataCollectionId dataCollectionId, long numRows);

    void rowsScanned(P partition, TableId tableId, long numRows);

    static <P extends TaskPartition> SnapshotProgressListener<P> NO_OP() {
        return new SnapshotProgressListener<P>() {
            @Override
            public void snapshotStarted(P partition) {
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
        };
    }
}
