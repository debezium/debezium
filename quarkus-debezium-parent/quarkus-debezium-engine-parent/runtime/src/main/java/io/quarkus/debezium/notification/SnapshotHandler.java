/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.SnapshotStatus;
import io.debezium.pipeline.spi.SnapshotResult;
import io.quarkus.debezium.notification.SnapshotEvent.Kind;

@ApplicationScoped
public class SnapshotHandler implements NotificationHandler {
    private final Event<SnapshotStarted> startedEvent;
    private final Event<SnapshotInProgress> inProgressEvent;
    private final Event<SnapshotTableScanCompleted> tableScanCompletedEvent;
    private final Event<SnapshotCompleted> completedEvent;
    private final Event<SnapshotAborted> abortedEvent;
    private final Event<SnapshotSkipped> skippedEvent;
    private final Event<SnapshotPaused> snapshotPausedEvent;
    private final Event<SnapshotResumed> snapshotResumedEvent;
    private final List<String> kinds = Arrays.stream(Kind.values()).map(Kind::getDescription).toList();

    @Inject
    public SnapshotHandler(Event<SnapshotStarted> startedEvent,
                           Event<SnapshotInProgress> inProgressEvent,
                           Event<SnapshotTableScanCompleted> tableScanCompletedEvent,
                           Event<SnapshotCompleted> completedEvent,
                           Event<SnapshotAborted> abortedEvent,
                           Event<SnapshotSkipped> skippedEvent,
                           Event<SnapshotPaused> snapshotPausedEvent,
                           Event<SnapshotResumed> snapshotResumedEvent) {
        this.startedEvent = startedEvent;
        this.inProgressEvent = inProgressEvent;
        this.tableScanCompletedEvent = tableScanCompletedEvent;
        this.completedEvent = completedEvent;
        this.abortedEvent = abortedEvent;
        this.skippedEvent = skippedEvent;
        this.snapshotPausedEvent = snapshotPausedEvent;
        this.snapshotResumedEvent = snapshotResumedEvent;
    }

    @Override
    public boolean isAvailable(String aggregateType) {
        return kinds.contains(aggregateType);
    }

    @Override
    public void handle(io.debezium.pipeline.notification.Notification notification) {
        Kind.from(notification.getAggregateType())
                .ifPresent(kind -> fire(kind, notification));
    }

    private void fire(Kind kind, Notification notification) {
        if (notification.getType().equals(SnapshotStatus.STARTED.name())) {
            startedEvent.fire(new SnapshotStarted(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotStatus.IN_PROGRESS.name())) {
            inProgressEvent.fire(new SnapshotInProgress(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotStatus.TABLE_SCAN_COMPLETED.name())) {
            tableScanCompletedEvent.fire(new SnapshotTableScanCompleted(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotStatus.COMPLETED.name())) {
            completedEvent.fire(new SnapshotCompleted(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotStatus.ABORTED.name())) {
            abortedEvent.fire(new SnapshotAborted(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotResult.SnapshotResultStatus.SKIPPED.name())) {
            skippedEvent.fire(new SnapshotSkipped(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotStatus.PAUSED.name())) {
            snapshotPausedEvent.fire(new SnapshotPaused(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }

        if (notification.getType().equals(SnapshotStatus.RESUMED.name())) {
            snapshotResumedEvent.fire(new SnapshotResumed(
                    notification.getId(),
                    notification.getAdditionalData(),
                    notification.getTimestamp(), kind));
        }
    }
}
