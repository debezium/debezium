/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.pipeline.spi.OffsetContext;

public abstract class RelationalOffsetContext implements OffsetContext {

    public abstract BaseSourceInfo getSourceInfoObject();

    public void markSnapshotRecord() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.TRUE);
    }

    public void markFirstSnapshotRecord() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.FIRST);
    }

    public void markFirstRecordInDataCollection() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
    }

    public void markLastSnapshotRecord() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.LAST);
    }

    public void markLastRecordInDataCollection() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.LAST_IN_DATA_COLLECTION);
    }

    public void incrementalSnapshotEvents() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    public void postSnapshotCompletion() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.FALSE);
    }
}
