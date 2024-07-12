/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DatabaseSchema;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support a pre-snapshot catch up streaming phase.
 */
public class PostgresChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChangeEventSourceCoordinator.class);

    private final Snapshotter snapshotter;
    private final SlotState slotInfo;

    private volatile boolean waitForSnapshotCompletion;

    public PostgresChangeEventSourceCoordinator(Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                ErrorHandler errorHandler,
                                                Class<? extends SourceConnector> connectorType,
                                                CommonConnectorConfig connectorConfig,
                                                PostgresChangeEventSourceFactory changeEventSourceFactory,
                                                ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
                                                EventDispatcher<PostgresPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                                                Snapshotter snapshotter, SlotState slotInfo,
                                                SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor, notificationService);
        this.snapshotter = snapshotter;
        this.slotInfo = slotInfo;
        this.waitForSnapshotCompletion = false;
    }

    @Override
    protected void executeChangeEventSources(CdcSourceTaskContext taskContext, SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource, Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext, ChangeEventSourceContext context)
            throws InterruptedException {
        final PostgresPartition partition = previousOffsets.getTheOnlyPartition();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
        SnapshotResult<PostgresOffsetContext> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);

        getSignalProcessor(previousOffsets).ifPresent(s -> s.setContext(snapshotResult.getOffset()));

        LOGGER.debug("Snapshot result {}", snapshotResult);

        if (context.isRunning() && snapshotResult.isCompletedOrSkipped()) {
            if(YugabyteDBServer.isEnabled() && !isSnapshotSkipped(snapshotResult)) {
                LOGGER.info("Will wait for snapshot completion before transitioning to streaming");
                waitForSnapshotCompletion = true;
                while (waitForSnapshotCompletion) {
                    LOGGER.debug("sleeping for 1s to receive snapshot completion offset");
                    Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.SYSTEM);
                    metronome.pause();
                    // Note: This heartbeat call is only required to support applications using debezium engine/embedded
                    // engine. It is not required when the connector is run with kakfa-connect.
                    eventDispatcher.alwaysDispatchHeartbeatEvent(partition, snapshotResult.getOffset());
                }
            }
            LOGGER.info("Transitioning to streaming");
            previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
            streamEvents(context, partition, snapshotResult.getOffset());
        }
    }

    protected boolean isSnapshotSkipped(SnapshotResult<PostgresOffsetContext> snapshotResult) {
        return snapshotResult.getStatus() == SnapshotResult.SnapshotResultStatus.SKIPPED;
    }

    @Override
    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                                             PostgresPartition partition,
                                                             PostgresOffsetContext previousOffset)
            throws InterruptedException {
        if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot() && slotInfo != null) {
            try {
                setSnapshotStartLsn((PostgresSnapshotChangeEventSource) snapshotSource,
                        previousOffset);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to determine catch-up streaming stopping LSN");
            }
            LOGGER.info("Previous connector state exists and will stream events until {} then perform snapshot",
                    previousOffset.getStreamingStoppingLsn());
            streamEvents(context, partition, previousOffset);
            return new CatchUpStreamingResult(true);
        }

        return new CatchUpStreamingResult(false);
    }

    private void setSnapshotStartLsn(PostgresSnapshotChangeEventSource snapshotSource,
                                     PostgresOffsetContext offsetContext)
            throws SQLException {
        snapshotSource.createSnapshotConnection();
        snapshotSource.setSnapshotTransactionIsolationLevel(false);
        snapshotSource.updateOffsetForPreSnapshotCatchUpStreaming(offsetContext);
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        if (YugabyteDBServer.isEnabled() && waitForSnapshotCompletion) {
            LOGGER.debug("Checking the offset value for snapshot completion");
            OffsetState offsetState = new PostgresOffsetContext.Loader((PostgresConnectorConfig) connectorConfig).load(offset).asOffsetState();
            if(!offsetState.snapshotInEffect()) {
                LOGGER.info("Offset conveys that snapshot has completed");
                waitForSnapshotCompletion = false;
            }
        }

        // This block won't be executed when we receive an offset that conveys that snapshot is completed because
        // streamingSource would be null. It is only initialised once we have transitioned to streaming. So, this
        // block would only be executed once we have switched to streaming phase.
        if (!commitOffsetLock.isLocked() && streamingSource != null && offset != null) {
            streamingSource.commitOffset(partition, offset);
        }
    }

}
