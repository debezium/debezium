/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.spi.schema.DataCollectionId;

public class InitialSnapshotNotificationService<P extends Partition, O extends OffsetContext> {

    public static final String INITIAL_SNAPSHOT = "Initial Snapshot";
    public static final String NONE = "<none>";
    public static final String CONNECTOR_NAME = "connector_name";
    public static final String STATUS = "status";

    private final NotificationService<P, O> notificationService;
    private final CommonConnectorConfig connectorConfig;

    public InitialSnapshotNotificationService(NotificationService<P, O> notificationService, CommonConnectorConfig connectorConfig) {
        this.notificationService = notificationService;
        this.connectorConfig = connectorConfig;
    }

    public <T extends DataCollectionId> void notifyStarted(P partition, OffsetContext offsetContext) {

        notificationService.notify(buildNotificationWith(SnapshotResult.SnapshotResultStatus.STARTED,
                Map.of()), Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyAborted(P partition, OffsetContext offsetContext) {

        notificationService.notify(buildNotificationWith(SnapshotResult.SnapshotResultStatus.ABORTED,
                Map.of()),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyCompleted(P partition, OffsetContext offsetContext) {

        notificationService.notify(buildNotificationWith(SnapshotResult.SnapshotResultStatus.COMPLETED,
                Map.of()),
                Offsets.of(partition, offsetContext));
    }

    public void notifySkipped(P partition, OffsetContext offsetContext) {

        notificationService.notify(buildNotificationWith(SnapshotResult.SnapshotResultStatus.SKIPPED,
                Map.of()),
                Offsets.of(partition, offsetContext));
    }

    private <T extends DataCollectionId> Notification buildNotificationWith(SnapshotResult.SnapshotResultStatus type,
                                                                            Map<String, String> additionalData) {

        Map<String, String> fullMap = new HashMap<>(additionalData);

        String connectorName = getConnectorName();
        fullMap.put(CONNECTOR_NAME, connectorName);

        return Notification.Builder.builder()
                .withId(UUID.randomUUID().toString())
                .withAggregateType(INITIAL_SNAPSHOT)
                .withType(type.name())
                .withAdditionalData(fullMap)
                .build();
    }

    private String getConnectorName() {

        return connectorConfig.getLogicalName();
    }
}
