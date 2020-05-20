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
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DatabaseSchema;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support a pre-snapshot catch up streaming phase.
 */
public class PostgresChangeEventSourceCoordinator extends ChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChangeEventSourceCoordinator.class);

    private final Snapshotter snapshotter;
    private final SlotState slotInfo;

    public PostgresChangeEventSourceCoordinator(OffsetContext previousOffset, ErrorHandler errorHandler,
                                                Class<? extends SourceConnector> connectorType,
                                                CommonConnectorConfig connectorConfig,
                                                ChangeEventSourceFactory changeEventSourceFactory,
                                                ChangeEventSourceMetricsFactory changeEventSourceMetricsFactory,
                                                EventDispatcher<?> eventDispatcher, DatabaseSchema<?> schema,
                                                Snapshotter snapshotter, SlotState slotInfo) {
        super(previousOffset, errorHandler, connectorType, connectorConfig, changeEventSourceFactory, changeEventSourceMetricsFactory, eventDispatcher, schema);
        this.snapshotter = snapshotter;
        this.slotInfo = slotInfo;
    }

    @Override
    protected CatchUpStreamingResult executeCatchUpStreaming(OffsetContext previousOffset, ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource snapshotSource)
            throws InterruptedException {
        if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot() && slotInfo != null) {
            try {
                setSnapshotStartLsn((PostgresSnapshotChangeEventSource) snapshotSource,
                        (PostgresOffsetContext) previousOffset);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to determine catch-up streaming stopping LSN");
            }
            LOGGER.info("Previous connector state exists and will stream events until {} then perform snapshot",
                    ((PostgresOffsetContext) previousOffset).getStreamingStoppingLsn());
            streamEvents(previousOffset, context);
            return new CatchUpStreamingResult(true);
        }

        return new CatchUpStreamingResult(false);
    }

    private void setSnapshotStartLsn(PostgresSnapshotChangeEventSource snapshotSource,
                                     PostgresOffsetContext offsetContext)
            throws SQLException {
        snapshotSource.createSnapshotConnection();
        snapshotSource.setSnapshotTransactionIsolationLevel();
        snapshotSource.updateOffsetForPreSnapshotCatchUpStreaming(offsetContext);
    }

}
