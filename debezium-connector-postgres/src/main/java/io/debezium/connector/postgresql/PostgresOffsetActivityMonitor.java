/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.time.Duration;
import java.util.Objects;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset commit and last completely processed log sequence numbers are compared against
 * the values captured when the monitor was last consulted, and when neither has moved, a
 * stale result is reported. The commit log sequence number is the position flushed to the
 * replication slot and therefore governs restart and WAL retention; the last completely
 * processed log sequence number is additionally compared so that progress within a single
 * large transaction is not reported as stale.
 *
 * @author Chris Cranford
 */
public class PostgresOffsetActivityMonitor implements OffsetActivityMonitor<PostgresPartition, PostgresOffsetContext> {

    private final Duration checkInterval;

    private Lsn previousCommitLsn;
    private Lsn previousProcessedLsn;

    public PostgresOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(PostgresPartition partition, PostgresOffsetContext offsetContext) {
        final Lsn commitLsn = offsetContext.lastCommitLsn();
        final Lsn processedLsn = offsetContext.lastCompletelyProcessedLsn();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if ((commitLsn != null || processedLsn != null)
                && Objects.equals(previousCommitLsn, commitLsn)
                && Objects.equals(previousProcessedLsn, processedLsn)) {
            result = StaleOffsetsResult.stale(
                    ("Offset commit LSN %s and processed LSN %s have not changed in %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that there are long running transaction(s) delaying the delivery of change events, " +
                            "which can delay the replication slot flush position and increase WAL retention.")
                            .formatted(commitLsn, processedLsn, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousCommitLsn = commitLsn;
        previousProcessedLsn = processedLsn;

        return result;
    }

}