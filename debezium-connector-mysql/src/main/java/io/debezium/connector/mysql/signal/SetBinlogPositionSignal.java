/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.document.Document;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

/**
 * Signal action that allows setting a custom binlog position for the MySQL connector.
 * This signal can be used to:
 * - Skip to a specific binlog file and position
 * - Skip to a specific GTID set
 * - Recover from a known good position after failures
 *
 * The signal expects data in one of these formats:
 * 1. For binlog file/position:
 *    {"binlog_filename": "mysql-bin.000003", "binlog_position": 1234}
 *
 * 2. For GTID:
 *    {"gtid_set": "server-uuid:1-100,other-uuid:1-50"}
 *
 * After the signal is processed, the new offset is persisted via a heartbeat event.
 * The connector must be restarted externally for the new position to take effect.
 *
 * @author Debezium Authors
 */
public class SetBinlogPositionSignal<P extends Partition> implements SignalAction<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SetBinlogPositionSignal.class);

    public static final String NAME = "set-binlog-position";

    private static final String BINLOG_FILENAME_KEY = "binlog_filename";
    private static final String BINLOG_POSITION_KEY = "binlog_position";
    private static final String GTID_SET_KEY = "gtid_set";

    private final EventDispatcher<P, ? extends DataCollectionId> eventDispatcher;

    public SetBinlogPositionSignal(EventDispatcher<P, ? extends DataCollectionId> eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        final Document data = signalPayload.data;

        if (data == null || data.isEmpty()) {
            LOGGER.warn("Received {} signal without data", NAME);
            return false;
        }

        LOGGER.info("Received {} signal: {}", NAME, data);

        try {
            // Validate and extract signal data
            final String binlogFilename = data.getString(BINLOG_FILENAME_KEY);
            final Long binlogPosition = data.getLong(BINLOG_POSITION_KEY);
            final String gtidSet = data.getString(GTID_SET_KEY);

            // Validate the signal data
            validateSignalData(binlogFilename, binlogPosition, gtidSet);

            // Get the current offset context
            MySqlOffsetContext offsetContext = (MySqlOffsetContext) signalPayload.offsetContext;
            if (offsetContext == null) {
                throw new DebeziumException("No offset context available for binlog position adjustment");
            }

            // Update the offset context with new position
            if (!Strings.isNullOrEmpty(gtidSet)) {
                LOGGER.info("Setting binlog position to GTID set: {}", gtidSet);
                offsetContext.setCompletedGtidSet(gtidSet);
            }
            else {
                LOGGER.info("Setting binlog position to file: {}, position: {}", binlogFilename, binlogPosition);
                offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
            }

            // Force a new offset commit to persist the change
            eventDispatcher.alwaysDispatchHeartbeatEvent(signalPayload.partition, offsetContext);

            LOGGER.info("Successfully updated binlog position. Restart the connector for changes to take effect.");

            return true;

        }
        catch (DebeziumException e) {
            // Re-throw DebeziumException without wrapping (includes our restart exception)
            throw e;
        }
        catch (Exception e) {
            LOGGER.error("Failed to process {} signal", NAME, e);
            throw new DebeziumException("Failed to set binlog position", e);
        }
    }

    private void validateSignalData(String binlogFilename, Long binlogPosition, String gtidSet) {
        // Check for mutual exclusivity
        boolean hasFilePosition = !Strings.isNullOrEmpty(binlogFilename) || binlogPosition != null;
        boolean hasGtid = !Strings.isNullOrEmpty(gtidSet);

        if (hasFilePosition && hasGtid) {
            throw new DebeziumException("Cannot specify both binlog file/position and GTID set");
        }

        if (!hasFilePosition && !hasGtid) {
            throw new DebeziumException("Must specify either binlog file/position or GTID set");
        }

        // Validate file/position
        if (hasFilePosition) {
            if (Strings.isNullOrEmpty(binlogFilename)) {
                throw new DebeziumException("Binlog filename must be specified when position is provided");
            }
            if (binlogPosition == null) {
                throw new DebeziumException("Binlog position must be specified when filename is provided");
            }
            if (binlogPosition < 0) {
                throw new DebeziumException("Binlog position must be non-negative");
            }
            if (!isValidBinlogFilename(binlogFilename)) {
                throw new DebeziumException("Invalid binlog filename format: " + binlogFilename);
            }
        }

        // Validate GTID
        if (hasGtid && !isValidGtidSet(gtidSet)) {
            throw new DebeziumException("Invalid GTID set format: " + gtidSet);
        }
    }

    private boolean isValidBinlogFilename(String filename) {
        // MySQL binlog files typically follow pattern: prefix.number (e.g., mysql-bin.000001)
        return filename.matches("^[a-zA-Z0-9_-]+\\.\\d{6}$");
    }

    private boolean isValidGtidSet(String gtidSet) {
        // Basic validation for GTID set format
        String gtidPattern = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:\\d+(-\\d+)?(,[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:\\d+(-\\d+)?)*$";
        return gtidSet.matches(gtidPattern);
    }
}