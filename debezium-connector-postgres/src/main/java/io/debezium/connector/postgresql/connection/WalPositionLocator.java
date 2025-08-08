/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;

/**
 * This class is responsible for finding out the LSN from which Debezium should resume streaming after the connector
 * restarts.
 * <p>
 * The order in which the changes appear in a logical replication connection may differ from the order in which they
 * were appended to the WAL. The WAL sender serializes parallel transactions and sends them to the client in the order
 * in which they were committed. Therefore, from the logical replication client's standpoint,
 * <ul>
 *     <li>Only the LSNs of transaction commits are ordered globally.</li>
 *     <li>The LSNs of changes are ordered only within their respective transaction.</li>
 * </ul>
 * It is thus necessary to find out the beginning of the unprocessed transaction and the LSN from which the streaming
 * should start.
 *
 * @author Jiri Pechanec
 *
 */
public class WalPositionLocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(WalPositionLocator.class);

    private final Lsn lastCommitStoredLsn;
    private final Lsn lastEventStoredLsn;
    private final Operation lastProcessedMessageType;
    private Lsn txStartLsn = null;
    private Lsn lsnAfterLastEventStoredLsn = null;
    private Lsn firstLsnReceived = null;
    private boolean passMessages = true;
    private Lsn startStreamingLsn = null;
    private boolean storeLsnAfterLastEventStoredLsn = false;

    public WalPositionLocator(Lsn lastCommitStoredLsn, Lsn lastEventStoredLsn, Operation lastProcessedMessageType) {
        this.lastCommitStoredLsn = lastCommitStoredLsn;
        this.lastEventStoredLsn = lastEventStoredLsn;
        this.lastProcessedMessageType = lastProcessedMessageType;

        LOGGER.info("Looking for WAL restart position for last commit LSN '{}' and last change LSN '{}'",
                lastCommitStoredLsn, lastEventStoredLsn);
    }

    public WalPositionLocator() {
        this.lastCommitStoredLsn = null;
        this.lastEventStoredLsn = null;
        this.lastProcessedMessageType = null;

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
                // If the current LSN is the same as the last event stored LSN, we need to resume from the next LSN
                if (lastProcessedMessageType == Operation.BEGIN || lastProcessedMessageType == Operation.COMMIT) {
                    // If the last processed message type is BEGIN or COMMIT, we can start streaming from the current LSN
                    // as it will prevent skipping of unprocessed event after BEGIN or previous tx COMMIT with same LSN
                    LOGGER.info("Will restart from LSN '{}' corresponding to the event following the last stored event", currentLsn);
                    startStreamingLsn = currentLsn;
                    return Optional.of(startStreamingLsn);
                }
                // If the last processed message type is not BEGIN or COMMIT, we need to wait for the next LSN
                LOGGER.info("Waiting for next LSN after last stored event '{}'", lastEventStoredLsn);
                return Optional.empty();
            }
            LOGGER.info("LSN after last stored change LSN '{}' received", currentLsn);
            startStreamingLsn = currentLsn;
            return Optional.of(startStreamingLsn);
        }

        if (currentLsn.equals(lastEventStoredLsn)) {
            storeLsnAfterLastEventStoredLsn = true;
        }

        if (lastEventStoredLsn == null) {
            startStreamingLsn = firstLsnReceived;
            return Optional.of(startStreamingLsn);
        }

        if (message.getOperation().equals(Operation.COMMIT)) {
            if (lastCommitStoredLsn == null || currentLsn.compareTo(lastCommitStoredLsn) > 0) {
                // If the current LSN is greater than the last commit stored LSN, and we haven't seen the
                // last event stored LSN yet, we can start streaming from the first LSN
                LOGGER.info("Will restart from LSN '{}' corresponding to the first LSN available", firstLsnReceived);
                startStreamingLsn = firstLsnReceived;
                return Optional.of(startStreamingLsn);
            }
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

    public Lsn getLastCommitStoredLsn() {
        return lastCommitStoredLsn;
    }

    @Override
    public String toString() {
        return "WalPositionLocator [lastCommitStoredLsn=" + lastCommitStoredLsn + ", lastEventStoredLsn="
                + lastEventStoredLsn + ", lastProcessedMessageType=" + lastProcessedMessageType + ", txStartLsn="
                + txStartLsn + ", lsnAfterLastEventStoredLsn=" + lsnAfterLastEventStoredLsn + ", firstLsnReceived="
                + firstLsnReceived + ", passMessages=" + passMessages + ", startStreamingLsn=" + startStreamingLsn
                + ", storeLsnAfterLastEventStoredLsn=" + storeLsnAfterLastEventStoredLsn + "]";
    }
}
