/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset and commit system change numbers are compared against the values captured when
 * the monitor was last consulted, and when the offset system change number has not moved, a
 * warning is logged and the SCN freeze metric is incremented.
 *
 * @author Chris Cranford
 */
public class LogMinerOffsetActivityMonitor implements OffsetActivityMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerOffsetActivityMonitor.class);

    private final Duration checkInterval;
    private final Supplier<OracleOffsetContext> offsetContextSupplier;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;
    private final Supplier<List<String>> activeTransactionIdSupplier;

    private Scn previousOffsetScn = Scn.NULL;
    private Map<Integer, Scn> previousCommitScns = new HashMap<>();

    public LogMinerOffsetActivityMonitor(Duration checkInterval,
                                         Supplier<OracleOffsetContext> offsetContextSupplier,
                                         LogMinerStreamingChangeEventSourceMetrics metrics,
                                         Supplier<List<String>> activeTransactionIdSupplier) {
        this.checkInterval = checkInterval;
        this.offsetContextSupplier = offsetContextSupplier;
        this.metrics = metrics;
        this.activeTransactionIdSupplier = activeTransactionIdSupplier;
    }

    @Override
    public void checkForStaleOffsets() {
        final OracleOffsetContext offsetContext = offsetContextSupplier.get();

        // Check for stale state
        if (offsetContext.getCommitScn() != null) {
            if (previousOffsetScn.equals(offsetContext.getScn())) {
                LOGGER.warn("Offset SCN {} has not changed in {} milliseconds. " +
                        "This may indicate long running transaction(s), active transactions: {}. Commit SCNs {}.",
                        previousOffsetScn, checkInterval.toMillis(), activeTransactionIdSupplier.get(), previousCommitScns);

                metrics.incrementScnFreezeCount();
            }
            else {
                metrics.setScnFreezeCount(0);
            }
        }

        // Update tracked stats
        previousOffsetScn = offsetContext.getScn();
        if (offsetContext.getCommitScn() != null) {
            previousCommitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
        }
    }

}