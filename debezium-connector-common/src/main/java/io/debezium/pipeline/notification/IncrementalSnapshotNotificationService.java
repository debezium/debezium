/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public static final String SCANNED_COLLECTION = "scanned_collection";
    public static final String CURRENT_COLLECTION_IN_PROGRESS = "current_collection_in_progress";
    public static final String MAXIMUM_KEY = "maximum_key";
    public static final String LAST_PROCESSED_KEY = "last_processed_key";
    public static final String NONE = "<none>";
    public static final String CONNECTOR_NAME = "connector_name";
    public static final String TOTAL_ROWS_SCANNED = "total_rows_scanned";
    public static final String TOTAL_ROWS = "total_rows";
    public static final String TOTAL_CHUNKS = "total_chunks";
    public static final String CHUNK_INDEX = "chunk_index";
    public static final String STATUS = "status";
    public static final String LIST_DELIMITER = ",";

    private final NotificationService<P, O> notificationService;

    private final CommonConnectorConfig connectorConfig;

    public Clock clock;

    public enum TableScanCompletionStatus {
        EMPTY,
        NO_PRIMARY_KEY,
        SKIPPED,
        SQL_EXCEPTION,
        SUCCEEDED,
        UNKNOWN_SCHEMA
    }

    public IncrementalSnapshotNotificationService(NotificationService<P, O> notificationService, CommonConnectorConfig config, Clock clock) {
        this.notificationService = notificationService;
        this.connectorConfig = config;
        this.clock = clock;
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

        String scannedCollection = incrementalSnapshotContext.currentDataCollectionId().getId().identifier();
        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        Map<String, String> additionalData = new HashMap<>();
        additionalData.put(DATA_COLLECTIONS, dataCollections);
        additionalData.put(SCANNED_COLLECTION, scannedCollection);
        additionalData.put(TOTAL_ROWS_SCANNED, String.valueOf(totalRowsScanned));
        additionalData.put(STATUS, status.name());
        // The best-effort per-collection total is reported only on a successful scan completion.
        if (status == TableScanCompletionStatus.SUCCEEDED) {
            incrementalSnapshotContext.totalRows().ifPresent(value -> additionalData.put(TOTAL_ROWS, String.valueOf(value)));
        }

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.TABLE_SCAN_COMPLETED,
                additionalData, offsetContext),
                Offsets.of(partition, offsetContext));
    }

    public <T extends DataCollectionId> void notifyInProgress(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext) {
        notifyInProgress(incrementalSnapshotContext, partition, offsetContext, 0L);
    }

    /**
     * Emits an {@code IN_PROGRESS} notification, adding best-effort per-collection progress fields when the context
     * carries a total row count. The derived {@code total_chunks} and {@code chunk_index} are computed here from that
     * single stored value (and the configured chunk size) so that the two snapshot sources (relational and MongoDB)
     * share one derivation.
     *
     * @param totalRowsScanned number of rows scanned so far, used to derive the current chunk index
     */
    public <T extends DataCollectionId> void notifyInProgress(IncrementalSnapshotContext<T> incrementalSnapshotContext, P partition, OffsetContext offsetContext,
                                                              long totalRowsScanned) {

        String dataCollections = incrementalSnapshotContext.getDataCollections().stream().map(DataCollection::getId)
                .map(DataCollectionId::identifier)
                .collect(Collectors.joining(LIST_DELIMITER));

        Map<String, String> additionalData = new HashMap<>();
        additionalData.put(DATA_COLLECTIONS, dataCollections);
        additionalData.put(CURRENT_COLLECTION_IN_PROGRESS, incrementalSnapshotContext.currentDataCollectionId().getId().identifier());
        additionalData.put(MAXIMUM_KEY,
                incrementalSnapshotContext.maximumKey().map(mk -> Arrays.stream(mk).map(x -> Objects.toString(x, "<null>")).collect(Collectors.joining(",")))
                        .orElse("\"<null>\""));
        additionalData.put(LAST_PROCESSED_KEY, Arrays.stream(incrementalSnapshotContext.chunkEndPosititon())
                .map(x -> Objects.toString(x, "<null>")).collect(Collectors.joining(",")));
        incrementalSnapshotContext.totalRows().ifPresent(rows -> {
            additionalData.put(TOTAL_ROWS, String.valueOf(rows));
            final int chunkSize = connectorConfig.getIncrementalSnapshotChunkSize();
            if (chunkSize > 0) {
                final long totalChunks = ceilDiv(rows, chunkSize);
                additionalData.put(TOTAL_CHUNKS, String.valueOf(totalChunks));
                additionalData.put(CHUNK_INDEX, String.valueOf(Math.min(ceilDiv(totalRowsScanned, chunkSize), totalChunks)));
            }
        });

        notificationService.notify(buildNotificationWith(incrementalSnapshotContext, SnapshotStatus.IN_PROGRESS,
                additionalData, offsetContext),
                Offsets.of(partition, offsetContext));
    }

    private static long ceilDiv(long value, long divisor) {
        return (value + divisor - 1) / divisor;
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
                .withTimestamp(Instant.now(clock).toEpochMilli())
                .build();
    }

}
