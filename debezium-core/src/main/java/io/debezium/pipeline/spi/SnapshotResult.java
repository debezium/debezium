/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

public class SnapshotResult {

    private final SnapshotResultStatus status;
    private final OffsetContext offset;

    private SnapshotResult(SnapshotResultStatus status, OffsetContext offset) {
        this.status = status;
        this.offset = offset;
    }

    public static SnapshotResult completed(OffsetContext offset) {
        return new SnapshotResult(SnapshotResultStatus.COMPLETED, offset);
    }

    public static SnapshotResult aborted() {
        return new SnapshotResult(SnapshotResultStatus.ABORTED, null);
    }

    public SnapshotResultStatus getStatus() {
        return status;
    }

    public OffsetContext getOffset() {
        return offset;
    }

    public static enum SnapshotResultStatus {
        COMPLETED,
        ABORTED;
    }
}
