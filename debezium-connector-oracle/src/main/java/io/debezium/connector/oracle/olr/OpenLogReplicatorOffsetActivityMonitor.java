/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset system change number is compared against the value captured when the monitor was
 * last consulted, and when it has not moved, a warning is logged. The offset position is only
 * advanced by changes that are dispatched, so a stationary position means no changes for the
 * captured tables have been emitted during the check interval.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorOffsetActivityMonitor implements OffsetActivityMonitor<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLogReplicatorOffsetActivityMonitor.class);

    private final Duration checkInterval;
    private final OpenLogReplicatorStreamingChangeEventSourceMetrics metrics;

    private Scn previousOffsetScn = Scn.NULL;

    public OpenLogReplicatorOffsetActivityMonitor(Duration checkInterval,
                                                  OpenLogReplicatorStreamingChangeEventSourceMetrics metrics) {
        this.checkInterval = checkInterval;
        this.metrics = metrics;
    }

    @Override
    public void checkForStaleOffsets(OraclePartition partition, OracleOffsetContext offsetContext) {
        final Scn offsetScn = offsetContext.getScn();

        // Check for stale state
        if (Objects.equals(previousOffsetScn, offsetScn)) {
            LOGGER.warn("Offset SCN {} has not changed in {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that there are long running transaction(s) delaying the delivery of change events.",
                    previousOffsetScn, checkInterval.toMillis());
            metrics.incrementWarningCount();
        }

        // Update tracked stats
        previousOffsetScn = offsetScn;
    }

}