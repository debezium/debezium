/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Field;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;

public interface SnapshotStatusNotification extends AutoCloseable {

    String DEFAULT_SNAPSHOT_NOTIFICATION_TOPIC_PROPERTY_NAME = "snapshot.status.notification.topic";

    Field SNAPSHOT_NOTIFICATION_TOPIC = Field.create(DEFAULT_SNAPSHOT_NOTIFICATION_TOPIC_PROPERTY_NAME)
            .withDisplayName("Topic name to send the snapshot notifications messages to.")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The topic name to send snapshot notification messages to. If not provided, not messages would be sent.");

    void updateStatus(Map<String, ?> partition, IncrementalSnapshotContext context, SnapshotStatus status, BlockingConsumer<SourceRecord> consumer)
            throws InterruptedException;

    @Override
    default void close() throws Exception {

    }
}
