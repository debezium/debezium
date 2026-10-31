/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.time.Duration;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.util.Clock;

/**
 * Tracks whether the search for the WAL resume position is receiving new LSNs from the server and
 * decides when a warning about a search that is not advancing should be logged.
 *
 * @author Chris Cranford
 */
class WalSearchProgressTracker {

    private final Clock clock;
    private final long warningIntervalMillis;
    private final long startMillis;

    private Lsn lastReceivedLsn;
    private long lastProgressMillis;
    private long lastWarningMillis;

    /**
     * @param clock the clock; may not be null
     * @param warningInterval the time without progress after which a warning is due; zero or negative disables warnings
     */
    WalSearchProgressTracker(Clock clock, Duration warningInterval) {
        this.clock = clock;
        this.warningIntervalMillis = warningInterval.toMillis();
        this.startMillis = clock.currentTimeInMillis();
        this.lastProgressMillis = startMillis;
        this.lastWarningMillis = startMillis;
    }

    /**
     * Records an LSN received from the server; the search counts as advancing only if the LSN is past
     * the last one received.
     *
     * @param lsn the received LSN; may be null
     */
    void lsnReceived(Lsn lsn) {
        if (lsn != null && lsn.isValid() && (lastReceivedLsn == null || lsn.compareTo(lastReceivedLsn) > 0)) {
            lastReceivedLsn = lsn;
            lastProgressMillis = clock.currentTimeInMillis();
            lastWarningMillis = lastProgressMillis;
        }
    }

    /**
     * Returns whether a warning should be logged because no new LSN arrived during the warning interval
     * since the last progress or the last warning. A {@code true} result restarts the interval.
     */
    boolean shouldWarn() {
        if (warningIntervalMillis <= 0) {
            return false;
        }
        final long now = clock.currentTimeInMillis();
        if (now - lastWarningMillis >= warningIntervalMillis) {
            lastWarningMillis = now;
            return true;
        }
        return false;
    }

    /**
     * @return the time since the search started or the last new LSN arrived, whichever is later
     */
    Duration timeWithoutProgress() {
        return Duration.ofMillis(clock.currentTimeInMillis() - lastProgressMillis);
    }

    /**
     * @return the time since the search started
     */
    Duration elapsed() {
        return Duration.ofMillis(clock.currentTimeInMillis() - startMillis);
    }

    /**
     * @return the last LSN received during the search, or null if none arrived
     */
    Lsn lastReceivedLsn() {
        return lastReceivedLsn;
    }
}
