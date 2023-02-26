/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;

public class SnapshotNotificationImpl implements SnapshotStatusNotification {
    @Override
    public void updateStatus(Map<String, ?> partition, IncrementalSnapshotContext context, SnapshotStatus status, BlockingConsumer<SourceRecord> consumer)
            throws InterruptedException {

    }
}
