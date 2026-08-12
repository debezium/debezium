/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

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
 * The offset system change number is compared against the value captured when the monitor was
 * last consulted, and when it has not moved, a stale result is reported. The offset position is
 * only advanced by changes that are dispatched, so a stationary position means no changes for
 * the captured tables have been emitted during the check interval.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorOffsetActivityMonitor implements OffsetActivityMonitor<OraclePartition, OracleOffsetContext> {

    private final Duration checkInterval;
    private final OpenLogReplicatorStreamingChangeEventSourceMetrics metrics;

    private Scn previousOffsetScn = Scn.NULL;

    public OpenLogReplicatorOffsetActivityMonitor(Duration checkInterval,
                                                  OpenLogReplicatorStreamingChangeEventSourceMetrics metrics) {
        this.checkInterval = checkInterval;
        this.metrics = metrics;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(OraclePartition partition, OracleOffsetContext offsetContext) {
        final Scn offsetScn = offsetContext.getScn();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (Objects.equals(previousOffsetScn, offsetScn)) {
            result = StaleOffsetsResult.stale(
                    ("Offset SCN %s has not changed in %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that there are long running transaction(s) delaying the delivery of change events.")
                            .formatted(previousOffsetScn, checkInterval.toMillis()));
            metrics.incrementWarningCount();
        }

        // Update tracked stats
        previousOffsetScn = offsetScn;

        return result;
    }

}