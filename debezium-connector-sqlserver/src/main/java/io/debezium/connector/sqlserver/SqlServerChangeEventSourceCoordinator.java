/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support snapshotting and streaming of multiple partitions.
 */
public class SqlServerChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerChangeEventSourceCoordinator.class);

    private final Clock clock;
    private final Duration pollInterval;

    private final AtomicBoolean firstStreamingIterationCompletedSuccessfully = new AtomicBoolean(false);

    public SqlServerChangeEventSourceCoordinator(Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets, ErrorHandler errorHandler,
                                                 Class<? extends SourceConnector> connectorType,
                                                 CommonConnectorConfig connectorConfig,
                                                 ChangeEventSourceFactory<SqlServerPartition, SqlServerOffsetContext> changeEventSourceFactory,
                                                 ChangeEventSourceMetricsFactory<SqlServerPartition> changeEventSourceMetricsFactory,
                                                 EventDispatcher<SqlServerPartition, ?> eventDispatcher,
                                                 DatabaseSchema<?> schema,
                                                 Clock clock,
                                                 SignalProcessor<SqlServerPartition, SqlServerOffsetContext> signalProcessor,
                                                 NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService,
                                                 SnapshotterService snapshotterService) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor, notificationService, snapshotterService);
        this.clock = clock;
        this.pollInterval = connectorConfig.getPollInterval();
    }

    public boolean firstStreamingIterationCompletedSuccessfully() {
        return firstStreamingIterationCompletedSuccessfully.get();
    }

    @Override
    protected void executeChangeEventSources(CdcSourceTaskContext taskContext, SnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> snapshotSource,
                                             Offsets<SqlServerPartition, SqlServerOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {
        Offsets<SqlServerPartition, SqlServerOffsetContext> streamingOffsets = Offsets.of(new HashMap<>());

        for (Map.Entry<SqlServerPartition, SqlServerOffsetContext> entry : previousOffsets) {
            SqlServerPartition partition = entry.getKey();
            SqlServerOffsetContext previousOffset = entry.getValue();

            previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
            SnapshotResult<SqlServerOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);

            if (snapshotResult.isCompletedOrSkipped()) {
                streamingOffsets.getOffsets().put(partition, snapshotResult.getOffset());
                if (previousOffsets.getOffsets().size() == 1) {
                    signalProcessor.setContext(snapshotResult.getOffset());
                }
                if (snapshotResult.isCompleted()) {
                    delayStreamingIfNeeded(context);
                }
            }
        }

        previousLogContext.set(taskContext.configureLoggingContext("streaming"));

        // TODO: Determine how to do incremental snapshots with multiple partitions
        for (Map.Entry<SqlServerPartition, SqlServerOffsetContext> entry : streamingOffsets) {
            initStreamEvents(entry.getKey(), entry.getValue());
        }

        getSignalProcessor(previousOffsets).ifPresent(signalProcessor -> registerSignalActionsAndStartProcessor(signalProcessor,
                eventDispatcher, this, connectorConfig));

        final Metronome metronome = Metronome.sleeper(pollInterval, clock);

        LOGGER.info("Starting streaming");

        while (context.isRunning()) {
            boolean streamedEvents = false;
            for (Map.Entry<SqlServerPartition, SqlServerOffsetContext> entry : streamingOffsets) {
                SqlServerPartition partition = entry.getKey();
                SqlServerOffsetContext previousOffset = entry.getValue();

                previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));

                if (context.isRunning()) {
                    streamedEvents = streamingSource.executeIteration(context, partition, previousOffset);
                }
            }

            if (!streamedEvents) {
                metronome.pause();
            }

            if (errorHandler.getProducerThrowable() == null) {
                firstStreamingIterationCompletedSuccessfully.set(true);
            }

            if (context.isPaused()) {
                LOGGER.info("Streaming will now pause");
                context.streamingPaused();
                context.waitSnapshotCompletion();
                LOGGER.info("Streaming resumed");
            }

        }

        LOGGER.info("Finished streaming");
    }
}
