/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class IncrementalSnapshotNotificationService<P extends Partition, O extends OffsetContext> {

    public static final String INCREMENTAL_SNAPSHOT = "Incremental Snapshot";
    public static final String DATA_COLLECTIONS = "data_collections";
    public static final String CURRENT_COLLECTION_IN_PROGRESS = "current_collection_in_progress";
    public static final String MAXIMUM_KEY = "maximum_key";
    public static final String LAST_PROCESSED_KEY = "last_processed_key";
    public static final String NONE = "<none>";
    public static final String CONNECTOR_NAME = "connector_name";
    public static final String TOTAL_ROWS_SCANNED = "total_rows_scanned";
    public static final String STATUS = "status";
    public static final String LIST_DELIMITER = ",";

    private final NotificationService<P, O> notificationService;

    private final CommonConnectorConfig connectorConfig;

    public enum TableScanCompletionStatus {
        EMPTY,
        NO_PRIMARY_KEY,
        SKIPPED,
        SQL_EXCEPTION,
        SUCCEEDED,
        UNKNOWN_SCHEMA
    }

    public IncrementalSnapshotNotificationService(NotificationService<P, O> notificationService, CommonConnectorConfig config) {
        this.notificationService = notificationService;
        connectorConfig = config;
    }

    public <T extends DataCollectionId> void notifyStarted(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {

        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.STARTED,
                Map.of(DATA_COLLECTIONS, dataCollections), offsetContext), Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyPaused(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {

        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.PAUSED,
                Map.of(DATA_COLLECTIONS, dataCollections), offsetContext),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyResumed(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {

        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.RESUMED,
                Map.of(DATA_COLLECTIONS, dataCollections), offsetContext),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyAborted(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.ABORTED,
                Map.of(), offsetContext), Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyAborted(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext,
                                                           List<String> dataCollectionIds) {

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.ABORTED,
                Map.of(DATA_COLLECTIONS, String.join(LIST_DELIMITER, dataCollectionIds)), offsetContext),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyTableScanCompleted(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext,
                                                                      long totalRowsScanned, TableScanCompletionStatus status) {

        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.TABLE_SCAN_COMPLETED,
                Map.of(
                        DATA_COLLECTIONS, dataCollections,
                        TOTAL_ROWS_SCANNED, String.valueOf(totalRowsScanned),
                        STATUS, status.name()),
                offsetContext),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyInProgress(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {

        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.IN_PROGRESS,
                Map.of(
                        DATA_COLLECTIONS, dataCollections,
                        CURRENT_COLLECTION_IN_PROGRESS, incrementalSnapshotContext.currentDataCollectionId().getId().identifier(),
                        MAXIMUM_KEY, incrementalSnapshotContext.maximumKey().orElse(new Object[0])[0].toString(),
                        LAST_PROCESSED_KEY, incrementalSnapshotContext.chunkEndPosititon()[0].toString()),
                offsetContext),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyCompleted(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.COMPLETED,
                Map.of(), offsetContext),
                Offsets.of(partition, offsetContext));
    }

    private <T extends DataCollectionId> Notification buildNotificationWith(IncrementalSnapshotContext<T> incrementalSnapshotContext, SnapshotStatus type,
                                                                            Map<String, String> additionalData, OffsetContext offsetContext) {

        Map<String, String> fullMap = new HashMap<>(additionalData);

        fullMap.put(CONNECTOR_NAME, connectorConfig.getLogicalName());

        String id = incrementalSnapshotContext.getCorrelationId() != null ? incrementalSnapshotContext.getCorrelationId() : UUID.randomUUID().toString();
        return Notification.Builder.builder()
                .withId(id)
                .withAggregateType(INCREMENTAL_SNAPSHOT)
                .withType(type.name())
                .withAdditionalData(fullMap)
                .build();
    }

}
