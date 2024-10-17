/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.Optional;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.pipeline.spi.OffsetContext;

public abstract class CommonOffsetContext<T extends BaseSourceInfo> implements OffsetContext {

    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    protected final T sourceInfo;
    protected SnapshotType snapshot;
    /**
     * Whether a snapshot has been completed or not.
     */
    protected boolean snapshotCompleted;

    public CommonOffsetContext(T sourceInfo) {
        this.sourceInfo = sourceInfo;
    }

    public CommonOffsetContext(T sourceInfo, boolean snapshotCompleted) {
        this.sourceInfo = sourceInfo;
        this.snapshotCompleted = snapshotCompleted;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        sourceInfo.setSnapshot(record);
    }

    @Override
    public boolean isSnapshotRunning() {
        return getSnapshot().isPresent() &&
                getSnapshot().get().equals(SnapshotType.INITIAL) &&
                !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart(boolean onDemand) {
        snapshot = onDemand ? SnapshotType.BLOCKING : SnapshotType.INITIAL;
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
        snapshot = null;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    public Optional<SnapshotType> getSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    public void setSnapshot(SnapshotType snapshotType) {
        snapshot = snapshotType;
    }
}
