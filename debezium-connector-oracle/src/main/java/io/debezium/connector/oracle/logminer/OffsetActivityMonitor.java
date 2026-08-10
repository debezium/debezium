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
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * A utility class that provides methods for tracking state changes to the connector's offsets.
 *
 * @author Chris Cranford
 */
public class OffsetActivityMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetActivityMonitor.class);

    private final Duration windowDuration;
    private final ElapsedTimeStrategy elapsedStrategy;
    private final OracleOffsetContext offsetContext;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;

    private Scn previousOffsetScn = Scn.NULL;
    private Map<Integer, Scn> previousCommitScns = new HashMap<>();

    public OffsetActivityMonitor(Duration windowDuration, OracleOffsetContext offsetContext, LogMinerStreamingChangeEventSourceMetrics metrics) {
        this.windowDuration = windowDuration;
        this.elapsedStrategy = ElapsedTimeStrategy.constant(Clock.SYSTEM, windowDuration);
        this.offsetContext = offsetContext;
        this.metrics = metrics;
    }

    /**
     * Checks for stale offsets.
     *
     * @param activeTransactionIdSupplier a non-{@code null} supplier that provides a list of active transaction ids
     */
    public void checkForStaleOffsets(Supplier<List<String>> activeTransactionIdSupplier) {
        // Check for stale state
        if (offsetContext.getCommitScn() != null) {
            final Scn currentScn = offsetContext.getScn();
            if (elapsedStrategy.hasElapsed()) {
                if (previousOffsetScn.equals(currentScn)) {
                    final List<String> activeTransactions = activeTransactionIdSupplier.get();
                    LOGGER.warn("Offset SCN {} has not changed in {} milliseconds. " +
                            "This may indicate long running transaction(s), active transactions: {}. Commit SCNs {}.",
                            previousOffsetScn, windowDuration.toMillis(), activeTransactions, previousCommitScns);

                    metrics.incrementScnFreezeCount();
                }
                else {
                    metrics.setScnFreezeCount(0);
                }
            }
        }

        // Update tracked stats
        previousOffsetScn = offsetContext.getScn();
        if (offsetContext.getCommitScn() != null) {
            previousCommitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
        }
    }

}
