/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

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
 * @author Pranav Tiwari
 *
 */
public class WalPositionLocatorStreaming implements PositionLocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(WalPositionLocatorStreaming.class);

    private final Lsn lastCommitStoredLsn;
    private final Lsn lastEventStoredLsn;
    private final ReplicationMessage.Operation lastProcessedMessageType;
    private final Long transactionId;
    private Lsn txStartLsn = null;
    private Lsn lsnAfterLastEventStoredLsn = null;
    private Lsn firstLsnReceived = null;
    private boolean passMessages = true;
    private Lsn startStreamingLsn = null;
    private boolean storeLsnAfterLastEventStoredLsn = false;
    private Set<Lsn> lsnSeen = new HashSet<>(1_000);

    public WalPositionLocatorStreaming(Lsn lastCommitStoredLsn, Lsn lastEventStoredLsn, ReplicationMessage.Operation lastProcessedMessageType, Long transactionId) {
        this.lastCommitStoredLsn = lastCommitStoredLsn;
        this.lastEventStoredLsn = lastEventStoredLsn;
        this.lastProcessedMessageType = lastProcessedMessageType;
        this.transactionId = transactionId;

        LOGGER.info("Looking for WAL restart position for last commit LSN '{}' and last change LSN '{}' and transaction id '{}' and lastProcessedMessageType '{}'",
                lastCommitStoredLsn, lastEventStoredLsn, transactionId, lastProcessedMessageType);
    }

    public WalPositionLocatorStreaming() {
        this.lastCommitStoredLsn = null;
        this.lastEventStoredLsn = null;
        this.lastProcessedMessageType = null;
        this.transactionId = null;

        LOGGER.info("WAL position will not be searched");
    }

    /**
     * Determines the appropriate Log Sequence Number (LSN) to resume streaming replication from,
     * based on the current LSN, the incoming replication message, and the transaction ID of a BEGIN message.
     * <p>
     * The method maintains internal state about the first LSN received and the last processed event,
     * and uses this information to decide whether to continue from the current LSN,
     * start from a previously stored position, or skip processing.
     * </p>
     *
     * @param currentLsn              the current LSN from the replication stream; must not be null.
     * @param message                 the replication message associated with the current LSN; must not be null.
     * @param beginMessageTransactionId the transaction ID associated with the BEGIN message; may be null.
     * @return an {@link Optional} containing the LSN to resume from, or empty if no resumption is appropriate at this point.
     */
    @Override
    public Optional<Lsn> resumeFromLsn(Lsn currentLsn, ReplicationMessage message, Long beginMessageTransactionId) {
        LOGGER.trace("Processing LSN '{}', operation '{}', lastCommitStoredLsn '{}', lastEventStoredLsn '{}', lastProcessedMessageType '{}'",
                currentLsn, message.getOperation(), lastCommitStoredLsn, lastEventStoredLsn, lastProcessedMessageType);

        if (firstLsnReceived == null) {
            firstLsnReceived = currentLsn;
            LOGGER.info("First LSN '{}' received", firstLsnReceived);
        }

        if (lastEventStoredLsn == null) {
            // we have not processed any message, we need to start from the current
            startStreamingLsn = firstLsnReceived;
            return Optional.of(startStreamingLsn);
        }

        if (lastProcessedMessageType.equals(ReplicationMessage.Operation.COMMIT)) {
            // In case we are polling for LSN that is of commit, then we will always get a record that is of next batch, if available.
            return Optional.of(currentLsn);
        }

        if (!transactionId.equals(beginMessageTransactionId)) {
            LOGGER.trace("While looking for lastProcessedLSN '{}', we are getting messages of different concurrent transaction due to protocol version 2 and all these messages will be processed after current un-processed messages of the transaction. ", lastEventStoredLsn);
            return Optional.empty();
        }

        // if we have reached so far, that means we have some processed transaction which we do not want to process again.
        lsnSeen.add(currentLsn);

        if (currentLsn.equals(lastEventStoredLsn)) {
            // Since we are adding this to lsnSeen, we will skip this lsn while checking and will process from next LSN.
            LOGGER.trace("Found the last processed LSN, processing will resume after this LSN.");
            startStreamingLsn = currentLsn;
            return Optional.of(startStreamingLsn);
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
    @Override
    public boolean skipMessage(Lsn lsn, Long beginMessageTransactionId) {
        if (passMessages) {
            return false;
        }
        if (lastEventStoredLsn == null) {
            LOGGER.trace("We have not processed any message, processing will start from LSN '{}', switching off the filtering", lsn);
            passMessages = true;
            return false;
        }
        if (!transactionId.equals(beginMessageTransactionId)) {
            LOGGER.trace("While looking for lastProcessedLSN '{}', we got a different transaction-id '{}' which will be processed.", lastCommitStoredLsn, beginMessageTransactionId);
            return false;
        }
        if (startStreamingLsn.equals(lsn)) {
            LOGGER.info("Message with LSN '{}' arrived and we have processed till this LSN. We will process from next LSN. switching off the filtering.", lsn);
            passMessages = true;
            lsnSeen = new HashSet<>(); // Empty the Map as it might be large and is no longer needed
            return true; // we will skip this message.
        }
        if (lsn.isValid() && !lsnSeen.contains(lsn)) {
            throw new DebeziumException(String.format(
                    "Message with LSN '%s' not present among LSNs seen in the location phase '%s'. This is unexpected and can lead to an infinite loop or a data loss.",
                    lsn,
                    lsnSeen));
        }
        LOGGER.debug("Message with LSN '{}' filtered", lsn);
        return true;
    }

    /**
     * Enables filtering of message LSNs based on calculated position.
     */
    public void enableFiltering() {
        LOGGER.info("wal position located. startgin streaming mode.");
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
        return "WalPositionLocatorStreaming{" +
                "lastCommitStoredLsn=" + lastCommitStoredLsn +
                ", lastEventStoredLsn=" + lastEventStoredLsn +
                ", lastProcessedMessageType=" + lastProcessedMessageType +
                ", transactionId=" + transactionId +
                ", txStartLsn=" + txStartLsn +
                ", lsnAfterLastEventStoredLsn=" + lsnAfterLastEventStoredLsn +
                ", firstLsnReceived=" + firstLsnReceived +
                ", passMessages=" + passMessages +
                ", startStreamingLsn=" + startStreamingLsn +
                ", storeLsnAfterLastEventStoredLsn=" + storeLsnAfterLastEventStoredLsn +
                ", lsnSeen=" + lsnSeen +
                '}';
    }
}
