/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for finding out a LSN from which Debezium should
 * resume streaming after connector restarts. The LSNs are not guaranteed to be
 * ordered in the WAL. LSN of commits are ordered and LSNs inside a transaction
 * are ordered. It is thus necessary to find out the beginning of the
 * unprocessed transaction and the LSN from which the streaming should start.
 *
 * @author Jiri Pechanec
 *
 */
public class WalPositionLocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(WalPositionLocator.class);

    private final Lsn lastCommitStoredLsn;
    private final Lsn lastEventStoredLsn;
    private Lsn txStartLsn = null;
    private Lsn lsnAfterLastEventStoredLsn = null;
    private Lsn firstLsnReceived = null;
    private boolean passMessages = true;
    private Lsn startStreamingLsn = null;
    private boolean storeLsnAfterLastEventStoredLsn = false;

    public WalPositionLocator(Lsn lastCommitStoredLsn, Lsn lastEventStoredLsn) {
        this.lastCommitStoredLsn = lastCommitStoredLsn;
        this.lastEventStoredLsn = lastEventStoredLsn;

        LOGGER.info("Looking for WAL restart position for last commit LSN '{}' and last change LSN '{}'",
                lastCommitStoredLsn, lastEventStoredLsn);
    }

    public WalPositionLocator() {
        this.lastCommitStoredLsn = null;
        this.lastEventStoredLsn = null;

        LOGGER.info("WAL position will not be searched");
    }

    /**
     * @return the first LSN from which processing should be started or empty if
     *         the position has not been found yet
     */
    public Optional<Lsn> resumeFromLsn(Lsn currentLsn, ReplicationMessage message) {
        LOGGER.trace("Processing LSN '{}', operation '{}'", currentLsn, message.getOperation());

        if (firstLsnReceived == null) {
            firstLsnReceived = currentLsn;
            LOGGER.info("First LSN '{}' received", firstLsnReceived);
        }
        if (storeLsnAfterLastEventStoredLsn) {
            // Event that immediately follows the last event seen
            // We can resume streaming from it
            if (currentLsn.equals(lastEventStoredLsn)) {
                // BEGIN and first message after change have the same LSN
                return Optional.empty();
            }
            lsnAfterLastEventStoredLsn = currentLsn;
            storeLsnAfterLastEventStoredLsn = false;
            LOGGER.info("LSN after last stored change LSN '{}' received", lsnAfterLastEventStoredLsn);
            startStreamingLsn = lsnAfterLastEventStoredLsn;
            return Optional.of(startStreamingLsn);
        }
        if (currentLsn.equals(lastEventStoredLsn)) {
            storeLsnAfterLastEventStoredLsn = true;
        }

        if (lastCommitStoredLsn == null) {
            startStreamingLsn = firstLsnReceived;
            return Optional.of(startStreamingLsn);
        }

        switch (message.getOperation()) {
            case BEGIN:
                txStartLsn = currentLsn;
                break;
            case COMMIT:
                if (currentLsn.compareTo(lastCommitStoredLsn) > 0) {
                    LOGGER.info("Received COMMIT LSN '{}' larger than than last stored commit LSN '{}'", currentLsn,
                            lastCommitStoredLsn);
                    if (lsnAfterLastEventStoredLsn != null) {
                        startStreamingLsn = lsnAfterLastEventStoredLsn;
                        LOGGER.info("Will restart from LSN '{}' that follows the lastest stored", startStreamingLsn);
                        return Optional.of(startStreamingLsn);
                    }
                    else if (txStartLsn != null) {
                        // Transaction that has not been processed and at the same
                        // time the event following the last seen was not met so
                        // replaying from the start of transaction
                        startStreamingLsn = txStartLsn;
                        LOGGER.info("Will restart from LSN '{}' that is start of the first unprocessed transaction", startStreamingLsn);
                        return Optional.of(startStreamingLsn);
                    }
                    else {
                        // Transaction that has not been processed and at the same
                        // time the event following the last seen was not met and
                        // transaction start event has not been met so replaying
                        // from the very first event
                        startStreamingLsn = firstLsnReceived;
                        LOGGER.info("Will restart from LSN '{}' that is the first LSN available", startStreamingLsn);
                        return Optional.of(startStreamingLsn);
                    }
                }
                break;
            default:
                break;
        }
        return Optional.empty();
    }

    /**
     * Decides whether the message with given LSN should be removed or not based on
     * previously located LSN point.
     *
     * @param lsn
     * @return true if the message should be skipped, false otherwise
     */
    public boolean skipMessage(Lsn lsn) {
        if (passMessages) {
            return false;
        }
        if (startStreamingLsn == null || startStreamingLsn.equals(lsn)) {
            LOGGER.info("Message with LSN '{}' arrived, switching off the filtering", lsn);
            passMessages = true;
            return false;
        }
        LOGGER.debug("Message with LSN '{}' filtered", lsn);
        return true;
    }

    /**
     * Enables filtering of message LSNs based on calculated position.
     */
    public void enableFiltering() {
        passMessages = false;
    }

    /**
     * @return true if searching of WAL position should be executed
     */
    public boolean searchingEnabled() {
        return lastEventStoredLsn != null;
    }

    public Lsn getLastEventStoredLsn() {
        return lastEventStoredLsn;
    }

    @Override
    public String toString() {
        return "WalPositionLocator [lastCommitStoredLsn=" + lastCommitStoredLsn + ", lastEventStoredLsn="
                + lastEventStoredLsn + ", txStartLsn=" + txStartLsn + ", lsnAfterLastEventStoredLsn="
                + lsnAfterLastEventStoredLsn + ", firstLsnReceived=" + firstLsnReceived + ", passMessages="
                + passMessages + ", startStreamingLsn=" + startStreamingLsn + ", storeLsnAfterLastEventStoredLsn="
                + storeLsnAfterLastEventStoredLsn + "]";
    }
}
