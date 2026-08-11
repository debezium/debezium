/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset binlog coordinates, the binlog filename and position along with the GTID set when
 * available, are compared against the values captured when the monitor was last consulted, and
 * when none have moved, a warning is logged. The binlog contains events for all databases
 * regardless of the connector's filters, so a stationary position means no events of any kind
 * have been received from the server during the check interval.
 *
 * @author Chris Cranford
 */
public class BinlogOffsetActivityMonitor<P extends BinlogPartition, O extends BinlogOffsetContext>
        implements OffsetActivityMonitor<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogOffsetActivityMonitor.class);

    private final Duration checkInterval;

    private String previousBinlogFilename;
    private Long previousBinlogPosition;
    private String previousGtidSet;

    public BinlogOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(P partition, O offsetContext) {
        final String binlogFilename = offsetContext.getSource().binlogFilename();
        final long binlogPosition = offsetContext.getSource().binlogPosition();
        final String gtidSet = offsetContext.gtidSet();

        // Check for stale state
        if (Objects.equals(previousBinlogFilename, binlogFilename)
                && Objects.equals(previousBinlogPosition, binlogPosition)
                && Objects.equals(previousGtidSet, gtidSet)) {
            LOGGER.warn("Offset binlog position {}/{} and GTID set {} have not changed in {} milliseconds. " +
                    "This may indicate the database is idle or that the connector is no longer receiving " +
                    "events from the server.",
                    binlogFilename, binlogPosition, gtidSet, checkInterval.toMillis());
        }

        // Update tracked stats
        previousBinlogFilename = binlogFilename;
        previousBinlogPosition = binlogPosition;
        previousGtidSet = gtidSet;
    }

}