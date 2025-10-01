/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import static io.debezium.pipeline.notification.SnapshotStatus.ABORTED;
import static io.debezium.pipeline.notification.SnapshotStatus.COMPLETED;
import static io.debezium.pipeline.notification.SnapshotStatus.IN_PROGRESS;
import static io.debezium.pipeline.notification.SnapshotStatus.PAUSED;
import static io.debezium.pipeline.notification.SnapshotStatus.RESUMED;
import static io.debezium.pipeline.notification.SnapshotStatus.STARTED;
import static io.debezium.pipeline.notification.SnapshotStatus.TABLE_SCAN_COMPLETED;
import static io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus.SKIPPED;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.notification.Notification;
import io.debezium.runtime.events.DefaultEngine;
import io.debezium.runtime.events.Engine;
import io.quarkus.debezium.engine.DebeziumThreadHandler;
import io.quarkus.debezium.notification.SnapshotEvent.Kind;

@ApplicationScoped
public class SnapshotHandler implements NotificationHandler {
    public static final BiConsumer<Kind, Notification> NO_EVENT = (a, b) -> {
    };
    private static final List<String> kinds = Arrays.stream(Kind.values()).map(Kind::getDescription).toList();

    private final Map<String, BiConsumer<Kind, Notification>> events;
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotHandler.class.getName());

    @Inject
    public SnapshotHandler(Event<SnapshotStarted> startedEvent,
                           Event<SnapshotInProgress> inProgressEvent,
                           Event<SnapshotTableScanCompleted> tableScanCompletedEvent,
                           Event<SnapshotCompleted> completedEvent,
                           Event<SnapshotAborted> abortedEvent,
                           Event<SnapshotSkipped> skippedEvent,
                           Event<SnapshotPaused> snapshotPausedEvent,
                           Event<SnapshotResumed> snapshotResumedEvent) {

        this.events = Map.of(
                STARTED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        startedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotStarted(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                IN_PROGRESS.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        inProgressEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotInProgress(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                TABLE_SCAN_COMPLETED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        tableScanCompletedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotTableScanCompleted(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                COMPLETED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        completedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotCompleted(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                ABORTED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        abortedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotAborted(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                SKIPPED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        skippedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotSkipped(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                PAUSED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        snapshotPausedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotPaused(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)),
                RESUMED.name(), (kind, notification) -> DefaultEngine.Literal.selectDefault(
                        snapshotResumedEvent
                                .select(Engine.Literal.of(DebeziumThreadHandler.context().manifest().id())),
                        DebeziumThreadHandler.context().manifest())
                        .fire(new SnapshotResumed(
                                notification.getId(),
                                notification.getAdditionalData(),
                                notification.getTimestamp(), kind)));
    }

    @Override
    public boolean isAvailable(String aggregateType) {
        return kinds.contains(aggregateType);
    }

    @Override
    public void handle(io.debezium.pipeline.notification.Notification notification) {
        Kind.from(notification.getAggregateType())
                .ifPresent(kind -> events.getOrDefault(notification.getType(), NO_EVENT).accept(kind, notification));
    }

}
