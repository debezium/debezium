/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.snapshot.Snapshotter;

/**
 * Abstract base class for all binlog-based source tasks.
 *
 * @author Chris Cranford
 */
public abstract class BinlogSourceTask<P extends Partition, O extends OffsetContext> extends BaseSourceTask<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogSourceTask.class);

    /**
     * Validates the feasibility of snapshot.
     *
     * @param snapshotter the snapshotter, should not be null
     * @param offsetContext the offset context, may be null
     * @param connection the database connection, should not be null
     * @return validates whether a snapshot can be taken
     */
    protected boolean validateSnapshotFeasibility(Snapshotter snapshotter, O offsetContext, BinlogConnectorConnection connection) {
        if (offsetContext == null) {
            if (!snapshotter.shouldSnapshotData(false, false)) {
                // Look to see what the first available binlog file is called, and whether it looks as though
                // binlog files have been purged. If so, then output warnings.
                final String earliestBinlogFileName = connection.earliestBinlogFilename();
                if (earliestBinlogFileName == null) {
                    LOGGER.warn("No binlog appears to be available. Ensure that the database's row-level binlog is enabled.");
                }
                else if (!earliestBinlogFileName.endsWith("00001")) {
                    LOGGER.warn("It is possible the server has purged some binlogs. "
                            + "If this is the case, then using snapshot mode may be required.");
                }
            }
        }
        return false;
    }

    /**
     * Validates the binlog configuration.
     *
     * @param snapshotter the snapshotter, should ont be null
     * @param connection the database connection, should not be null
     */
    protected void validateBinlogConfiguration(Snapshotter snapshotter, BinlogConnectorConnection connection) {
        if (snapshotter.shouldStream()) {
            // Check whether the row-level binlog is enabled ...
            if (!connection.isBinlogFormatRow()) {
                throw new DebeziumException("The database server is not configured to use a ROW binlog_format, which is "
                        + "required for this connector to work properly. Change the database configuration to use a "
                        + "binlog_format=ROW and restart the connector.");
            }
            if (!connection.isBinlogRowImageFull()) {
                throw new DebeziumException("The database server is not configured to use a FULL binlog_row_image, which is "
                        + "required for this connector to work properly. Change the database configuration to use a "
                        + "binlog_row_image=FULL and restart the connector.");
            }
        }
    }

    /**
     * Reset the specified offset.
     *
     * @param connectorConfig the connector configuration, should not be null
     * @param previousOffset the previous offsets
     * @param signalProcessor the signal processor, should not be null
     */
    protected void resetOffset(BinlogConnectorConfig connectorConfig, O previousOffset, SignalProcessor<P, O> signalProcessor) {
        boolean isKafkaChannelEnabled = connectorConfig.getEnabledChannels().contains(KafkaSignalChannel.CHANNEL_NAME);
        if (previousOffset != null && isKafkaChannelEnabled && connectorConfig.isReadOnlyConnection()) {
            KafkaSignalChannel kafkaSignal = signalProcessor.getSignalChannel(KafkaSignalChannel.class);
            Long signalOffset = getReadOnlyIncrementalSnapshotSignalOffset(previousOffset);
            if (signalOffset != null) {
                LOGGER.info("Resetting Kafka Signal offset to {}", signalOffset);
                kafkaSignal.reset(signalOffset);
            }
        }
    }

    /**
     * Obtain the read-only incremental snapshot signal offset.
     *
     * @param previousOffset the previous offsets
     * @return the read-only incremental snapshot signal offset
     */
    protected abstract Long getReadOnlyIncrementalSnapshotSignalOffset(O previousOffset);

    /**
     * Common heartbeat error handler for binlog-based connectors.
     */
    public static class BinlogHeartbeatErrorHandler implements HeartbeatErrorHandler {
        @Override
        public void onError(SQLException exception) throws RuntimeException {
            final String sqlErrorId = exception.getSQLState();
            switch (sqlErrorId) {
                case "42000":
                    // error_er_dbaccess_denied_error, see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_dbaccess_denied_error
                    throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                case "3D000":
                    // error_er_no_db_error, see https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_no_db_error
                    throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                default:
                    break;
            }
        }
    }
}
