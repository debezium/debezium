/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.spi.schema.DataCollectionId;

public interface SnapshotStatusNotifications<T extends DataCollectionId> {
    String CONFIGURATION_FIELD_PREFIX_STRING = "snapshot.status.notifications.";

    Field SNAPSHOT_NOTIFICATIONS_CLASS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "class")
            .withDisplayName("Snapshot notifications class")
            .withType(ConfigDef.Type.CLASS)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("The name of the SnapshotStatusNotifications class that should be used to output the snapshot status updates. "
                    + "The configuration properties for the snapshot notifications are prefixed with the '"
                    + CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
            .withDefault(LogSnapshotStatusNotifications.class.getName());

    void configure(Configuration config);

    void snapshotCompleted(String connectorName, DataCollection<T> dataCollection, SnapshotCompletionStatus status);
}
