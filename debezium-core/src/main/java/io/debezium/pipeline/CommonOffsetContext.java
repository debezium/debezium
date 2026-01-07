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

    /**
     * Indicates the type of in progress snapshot (INITIAL, BLOCKING, INCREMENTAL).
     * In case of an INITIAL or BLOCKING snapshot it will be used in conjunction with {@link #snapshotCompleted}
     * to define if it is running or not.
     * <p>
     * The following table lists the possible status:
     * <table border="3">
     * <tr><th>Status</th><th>snapshot</th><th>snapshotCompleted</th></tr>
     * <tr><td>incomplete initial snapshot</td><td>initial</td><td>false</td></tr>
     * <tr><td>completed initial snapshot</td><td>null</td><td>true</td></tr>
     * <tr><td>incomplete blocking snapshot</td><td>blocking</td><td>false</td></tr>
     * <tr><td>completed blocking snapshot</td><td>null</td><td>true</td></tr>
     * <tr><td>running incremental snapshot</td><td>incremental</td><td>true</td></tr>
     * <tr><td>completed incremental snapshot</td><td>null</td><td>true</td></tr>
     * </table>
     *
     */
    protected SnapshotType snapshot;

    /**
     * Whether an initial/blocking snapshot has been completed or not.
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
    public boolean isInitialSnapshotRunning() {
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
        snapshot = SnapshotType.INCREMENTAL;
    }

    public Optional<SnapshotType> getSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    public void setSnapshot(SnapshotType snapshotType) {
        snapshot = snapshotType;
    }
}
