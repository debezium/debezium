/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.pipeline.spi.OffsetContext;

public abstract class CommonOffsetContext implements OffsetContext {

    public abstract BaseSourceInfo getSourceInfoObject();

    @Override
    public Struct getSourceInfo() {
        return getSourceInfoObject().struct();
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        getSourceInfoObject().setSnapshot(record);
    }

    @Override
    public void postSnapshotCompletion() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public void incrementalSnapshotEvents() {
        getSourceInfoObject().setSnapshot(SnapshotRecord.INCREMENTAL);
    }
}
