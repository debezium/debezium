/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

public class SnapshotResult {

    private final SnapshotResultStatus status;
    private final OffsetContext offset;

    public SnapshotResult(SnapshotResultStatus status, OffsetContext offset) {
        this.status = status;
        this.offset = offset;
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
