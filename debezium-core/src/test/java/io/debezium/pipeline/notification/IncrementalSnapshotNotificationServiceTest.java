/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;

@RunWith(MockitoJUnitRunner.class)
public class IncrementalSnapshotNotificationServiceTest {

    @Mock
    private Partition partition;
    @Mock
    private OffsetContext offsetContext;
    @Mock
    private NotificationService<Partition, OffsetContext> notificationService;
    @Mock
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    @InjectMocks
    private IncrementalSnapshotNotificationService<Partition, OffsetContext> incrementalSnapshotNotificationService;

    @Before
    public void setUp() {
        Struct mockedSourceStruct = mock(Struct.class);
        when(mockedSourceStruct.getString("name")).thenReturn("connector-test");
        when(offsetContext.getSourceInfo()).thenReturn(mockedSourceStruct);
        when(incrementalSnapshotContext.getCorrelationId()).thenReturn("12345");
        when(incrementalSnapshotContext.getDataCollections()).thenReturn(List.of(
                new DataCollection<>(new TableId("db", "inventory", "product")),
                new DataCollection<>(new TableId("db", "inventory", "customer"))));
        when(incrementalSnapshotContext.currentDataCollectionId()).thenReturn(new DataCollection<>(new TableId("db", "inventory", "product")));
        when(incrementalSnapshotContext.maximumKey()).thenReturn(Optional.of(new Object[]{ 100, 0, 0 }));
        when(incrementalSnapshotContext.chunkEndPosititon()).thenReturn(new Object[]{ 50, 0, 0 });
    }

    @Test
    public void notifyStarted() {

        incrementalSnapshotNotificationService.notifyStarted(incrementalSnapshotContext, partition, offsetContext);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "STARTED", Map.of(
                "connector_name", "connector-test",
                "data_collections", "db.inventory.product,db.inventory.customer"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void notifyPaused() {

        incrementalSnapshotNotificationService.notifyPaused(incrementalSnapshotContext, partition, offsetContext);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "PAUSED", Map.of(
                "connector_name", "connector-test",
                "data_collections", "db.inventory.product,db.inventory.customer"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void notifyResumed() {

        incrementalSnapshotNotificationService.notifyResumed(incrementalSnapshotContext, partition, offsetContext);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "RESUMED", Map.of(
                "connector_name", "connector-test",
                "data_collections", "db.inventory.product,db.inventory.customer"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void notifyAborted() {

        incrementalSnapshotNotificationService.notifyAborted(incrementalSnapshotContext, partition, offsetContext);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "ABORTED", Map.of(
                "connector_name", "connector-test"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void testNotifyAborted() {

        incrementalSnapshotNotificationService.notifyAborted(incrementalSnapshotContext, partition, offsetContext, List.of("db.inventory.product"));

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "ABORTED", Map.of(
                "connector_name", "connector-test",
                "data_collections", "db.inventory.product"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void notifyTableScanCompleted() {

        incrementalSnapshotNotificationService.notifyTableScanCompleted(incrementalSnapshotContext, partition, offsetContext, 100L);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "TABLE_SCAN_COMPLETED", Map.of(
                "connector_name", "connector-test",
                "data_collections", "db.inventory.product,db.inventory.customer",
                "total_rows_scanned", "100"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void notifyInProgress() {

        incrementalSnapshotNotificationService.notifyInProgress(incrementalSnapshotContext, partition, offsetContext);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "IN_PROGRESS", Map.of(
                "connector_name", "connector-test",
                "data_collections", "db.inventory.product,db.inventory.customer",
                "current_collection_in_progress", "db.inventory.product",
                "maximum_key", "100",
                "last_processed_key", "50"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));
    }

    @Test
    public void notifyCompleted() {

        incrementalSnapshotNotificationService.notifyCompleted(incrementalSnapshotContext, partition, offsetContext);

        Notification expectedNotification = new Notification("12345", "Incremental Snapshot", "COMPLETED", Map.of(
                "connector_name", "connector-test"));

        verify(notificationService).notify(eq(expectedNotification), any(Offsets.class));

    }
}
