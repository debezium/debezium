/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.spi.SlotState;
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
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotterService;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support a pre-snapshot catch up streaming phase.
 */
public class PostgresChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChangeEventSourceCoordinator.class);

    private final SnapshotterService snapshotterService;
    private final SlotState slotInfo;

    public PostgresChangeEventSourceCoordinator(Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets,
                                                ErrorHandler errorHandler,
                                                Class<? extends SourceConnector> connectorType,
                                                CommonConnectorConfig connectorConfig,
                                                PostgresChangeEventSourceFactory changeEventSourceFactory,
                                                ChangeEventSourceMetricsFactory<PostgresPartition> changeEventSourceMetricsFactory,
                                                EventDispatcher<PostgresPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                                                SnapshotterService snapshotterService, SlotState slotInfo,
                                                SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor,
                                                NotificationService<PostgresPartition, PostgresOffsetContext> notificationService, BeanRegistry beanRegistry,
                                                ServiceRegistry serviceRegistry) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor, notificationService, snapshotterService, beanRegistry, serviceRegistry);
        this.snapshotterService = snapshotterService;
        this.slotInfo = slotInfo;
    }

    @Override
    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> snapshotSource,
                                                             PostgresPartition partition,
                                                             PostgresOffsetContext previousOffset)
            throws InterruptedException {
        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot() && slotInfo != null) {
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

}
