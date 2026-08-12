/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.time.Duration;
import java.util.Objects;

import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset LCR position is compared against the value captured when the monitor was last
 * consulted, and when the position has not moved, a stale result is reported. When the offsets
 * do not yet contain an LCR position, i.e. streaming has not observed the first change event,
 * the system change number is compared instead.
 *
 * @author Chris Cranford
 */
public class XStreamOffsetActivityMonitor implements OffsetActivityMonitor<OraclePartition, OracleOffsetContext> {

    private final Duration checkInterval;
    private final XStreamStreamingChangeEventSourceMetrics metrics;

    private String previousLcrPosition;
    private Scn previousOffsetScn = Scn.NULL;

    public XStreamOffsetActivityMonitor(Duration checkInterval, XStreamStreamingChangeEventSourceMetrics metrics) {
        this.checkInterval = checkInterval;
        this.metrics = metrics;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(OraclePartition partition, OracleOffsetContext offsetContext) {
        final String lcrPosition = offsetContext.getLcrPosition();
        final Scn offsetScn = offsetContext.getScn();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (lcrPosition != null) {
            if (Objects.equals(previousLcrPosition, lcrPosition)) {
                result = StaleOffsetsResult.stale(
                        ("Offset LCR position %s has not changed in %d milliseconds. " +
                                "This may indicate the database is idle or that there are long running transaction(s) " +
                                "delaying the delivery of change events.")
                                .formatted(previousLcrPosition, checkInterval.toMillis()));
                metrics.incrementWarningCount();
            }
        }
        else if (previousLcrPosition == null && Objects.equals(previousOffsetScn, offsetScn)) {
            // No LCR position has been observed yet; fallback to comparing the offset SCN
            result = StaleOffsetsResult.stale(
                    ("Offset SCN %s has not changed in %d milliseconds and no LCR position has been " +
                            "received. This may indicate the database is idle or that there are long running " +
                            "transaction(s) delaying the delivery of change events.")
                            .formatted(previousOffsetScn, checkInterval.toMillis()));
            metrics.incrementWarningCount();
        }

        // Update tracked stats
        previousLcrPosition = lcrPosition;
        previousOffsetScn = offsetScn;

        return result;
    }

}