/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset commit and last completely processed log sequence numbers are compared against
 * the values captured when the monitor was last consulted, and when neither has moved, a
 * warning is logged. The commit log sequence number is the position flushed to the replication
 * slot and therefore governs restart and WAL retention; the last completely processed log
 * sequence number is additionally compared so that progress within a single large transaction
 * is not reported as stale.
 *
 * @author Chris Cranford
 */
public class PostgresOffsetActivityMonitor implements OffsetActivityMonitor<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresOffsetActivityMonitor.class);

    private final Duration checkInterval;

    private Lsn previousCommitLsn;
    private Lsn previousProcessedLsn;

    public PostgresOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(PostgresPartition partition, PostgresOffsetContext offsetContext) {
        final Lsn commitLsn = offsetContext.lastCommitLsn();
        final Lsn processedLsn = offsetContext.lastCompletelyProcessedLsn();

        // Check for stale state
        if ((commitLsn != null || processedLsn != null)
                && Objects.equals(previousCommitLsn, commitLsn)
                && Objects.equals(previousProcessedLsn, processedLsn)) {
            LOGGER.warn("Offset commit LSN {} and processed LSN {} have not changed in {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that there are long running transaction(s) delaying the delivery of change events, " +
                    "which can delay the replication slot flush position and increase WAL retention.",
                    commitLsn, processedLsn, checkInterval.toMillis());
        }

        // Update tracked stats
        previousCommitLsn = commitLsn;
        previousProcessedLsn = processedLsn;
    }

}