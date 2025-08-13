/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Map;

import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.pipeline.notification.Notification;

class SnapshotHandlerTest {

    private final Event<SnapshotStarted> startedEvent = Mockito.mock(Event.class);
    private final Event<SnapshotInProgress> inProgressEvent = Mockito.mock(Event.class);
    private final Event<SnapshotTableScanCompleted> tableScanCompletedEvent = Mockito.mock(Event.class);
    private final Event<SnapshotCompleted> completedEvent = Mockito.mock(Event.class);
    private final Event<SnapshotAborted> abortedEvent = Mockito.mock(Event.class);
    private final Event<SnapshotSkipped> skippedEvent = Mockito.mock(Event.class);
    private final Event<SnapshotPaused> snapshotPausedEvent = Mockito.mock(Event.class);
    private final Event<SnapshotResumed> snapshotResumedEvent = Mockito.mock(Event.class);

    @Test
    @DisplayName("should fire snapshotStarted when snapshot started")
    void shouldFireSnapshotStartedWhenNotificationIsSnapshotStarted() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "STARTED", Map.of(
                "aKey", "aValue"), 1L));

        verify(startedEvent, times(1)).fire(new SnapshotStarted(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotPausedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire snapshotInProgress when snapshot in progress")
    void shouldFireSnapshotInProgressWhenNotificationIsSnapshotInProgress() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "IN_PROGRESS", Map.of(
                "aKey", "aValue"), 1L));

        verify(inProgressEvent, times(1)).fire(new SnapshotInProgress(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotPausedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire tableScanCompletedEvent when snapshot table scan completed")
    void shouldFireSnapshotTableScanCompletedEventWhenNotificationIsSnapshotTableScanCompleted() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "TABLE_SCAN_COMPLETED", Map.of(
                "aKey", "aValue"), 1L));

        verify(tableScanCompletedEvent, times(1)).fire(new SnapshotTableScanCompleted(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotPausedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire completedEvent when snapshot is completed")
    void shouldFireSnapshotCompletedEventEventWhenNotificationIsSnapshotIsCompleted() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "COMPLETED", Map.of(
                "aKey", "aValue"), 1L));

        verify(completedEvent, times(1)).fire(new SnapshotCompleted(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotPausedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire abortedEvent when snapshot is aborted")
    void shouldFireSnapshotAbortedEventEventWhenNotificationIsSnapshotIsAborted() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "ABORTED", Map.of(
                "aKey", "aValue"), 1L));

        verify(abortedEvent, times(1)).fire(new SnapshotAborted(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotPausedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire skippedEvent when snapshot is skipped")
    void shouldFireSnapshotSkippedEventEventWhenNotificationIsSnapshotIsSkipped() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "SKIPPED", Map.of(
                "aKey", "aValue"), 1L));

        verify(skippedEvent, times(1)).fire(new SnapshotSkipped(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(snapshotPausedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire snapshotPausedEvent when snapshot is paused")
    void shouldFireSnapshotPausedEventEventEventWhenNotificationIsSnapshotIsPaused() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "PAUSED", Map.of(
                "aKey", "aValue"), 1L));

        verify(snapshotPausedEvent, times(1)).fire(new SnapshotPaused(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotResumedEvent);
    }

    @Test
    @DisplayName("should fire snapshotResumedEvent when snapshot is resumed")
    void shouldFireSnapshotResumedEventEventEventWhenNotificationIsSnapshotIsResumed() {
        SnapshotHandler underTest = new SnapshotHandler(
                startedEvent,
                inProgressEvent,
                tableScanCompletedEvent,
                completedEvent,
                abortedEvent,
                skippedEvent,
                snapshotPausedEvent,
                snapshotResumedEvent);

        underTest.handle(new Notification("id", "Initial Snapshot", "RESUMED", Map.of(
                "aKey", "aValue"), 1L));

        verify(snapshotResumedEvent, times(1)).fire(new SnapshotResumed(
                "id",
                Map.of("aKey", "aValue"),
                1L, SnapshotEvent.Kind.INITIAL));

        verifyNoInteractions(startedEvent);
        verifyNoInteractions(inProgressEvent);
        verifyNoInteractions(tableScanCompletedEvent);
        verifyNoInteractions(completedEvent);
        verifyNoInteractions(abortedEvent);
        verifyNoInteractions(skippedEvent);
        verifyNoInteractions(snapshotPausedEvent);
    }
}
