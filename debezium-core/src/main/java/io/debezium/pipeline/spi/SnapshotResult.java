/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

public class SnapshotResult {

    private final OffsetContext offset;

    public SnapshotResult(OffsetContext offset) {
        this.offset = offset;
    }

    public OffsetContext getOffset() {
        return offset;
    }
}
