/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.time.Duration;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset binlog coordinates, the binlog filename and position along with the GTID set when
 * available, are compared against the values captured when the monitor was last consulted, and
 * when none have moved, a stale result is reported. The binlog contains events for all databases
 * regardless of the connector's filters, so a stationary position means no events of any kind
 * have been received from the server during the check interval.
 *
 * @author Chris Cranford
 */
public class BinlogOffsetActivityMonitor<P extends BinlogPartition, O extends BinlogOffsetContext>
        implements OffsetActivityMonitor<P, O> {

    private final Duration checkInterval;

    private String previousBinlogFilename;
    private Long previousBinlogPosition;
    private String previousGtidSet;

    public BinlogOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(P partition, O offsetContext) {
        final String binlogFilename = offsetContext.getSource().binlogFilename();
        final long binlogPosition = offsetContext.getSource().binlogPosition();
        final String gtidSet = offsetContext.gtidSet();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (Objects.equals(previousBinlogFilename, binlogFilename)
                && Objects.equals(previousBinlogPosition, binlogPosition)
                && Objects.equals(previousGtidSet, gtidSet)) {
            result = StaleOffsetsResult.stale(
                    ("Offset binlog position %s/%d and GTID set %s have not changed in %d milliseconds. " +
                            "This may indicate the database is idle or that the connector is no longer receiving " +
                            "events from the server.")
                            .formatted(binlogFilename, binlogPosition, gtidSet, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousBinlogFilename = binlogFilename;
        previousBinlogPosition = binlogPosition;
        previousGtidSet = gtidSet;

        return result;
    }

}