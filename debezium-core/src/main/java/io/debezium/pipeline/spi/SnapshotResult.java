/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

public class SnapshotResult<O extends OffsetContext> {

    private final SnapshotResultStatus status;
    private final O offset;

    private SnapshotResult(SnapshotResultStatus status, O offset) {
        this.status = status;
        this.offset = offset;
    }

    public static <O extends OffsetContext> SnapshotResult<O> completed(O offset) {
        return new SnapshotResult<>(SnapshotResultStatus.COMPLETED, offset);
    }

    public static <O extends OffsetContext> SnapshotResult<O> aborted() {
        return new SnapshotResult<>(SnapshotResultStatus.ABORTED, null);
    }

    public static <O extends OffsetContext> SnapshotResult<O> skipped(O offset) {
        return new SnapshotResult<>(SnapshotResultStatus.SKIPPED, offset);
    }

    public boolean isCompletedOrSkipped() {
        return this.status == SnapshotResultStatus.SKIPPED || this.status == SnapshotResultStatus.COMPLETED;
    }

    public SnapshotResultStatus getStatus() {
        return status;
    }

    public O getOffset() {
        return offset;
    }

    public enum SnapshotResultStatus {
        STARTED,

        COMPLETED,
        ABORTED,
        SKIPPED
    }

    @Override
    public String toString() {
        return "SnapshotResult [status=" + status + ", offset=" + offset + "]";
    }
}
