/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

public enum SnapshotStatus {
    STARTED,
    PAUSED,
    RESUMED,
    ABORTED,
    IN_PROGRESS,
    TABLE_CHUNK_IN_PROGRESS,
    TABLE_CHUNK_COMPLETED,
    TABLE_SCAN_COMPLETED,
    COMPLETED
}
