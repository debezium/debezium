/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.util.Optional;

/**
 * Defines the contract for locating or tracking a position within a stream of PostgreSQL replication messages.
 * <p>
 * Implementations of this interface can determine whether to resume processing from a given LSN,
 * skip certain messages, or enable/disable filtering logic based on replication metadata.
 * </p>
 *
 * This abstraction allows handling both streaming and non-streaming replication modes uniformly.
 *
 * @author Pranav Tiwari
 */
public interface PositionLocator {

    /**
     * Determines whether to resume processing from the current LSN based on the given replication message.
     *
     * @param currentLsn the LSN of the current replication message
     * @param message the replication message to evaluate
     * @param beginMessageTransactionId the transaction ID associated with the current message's BEGIN event
     * @return an {@link Optional} containing the LSN to resume from, or empty if processing should continue
     */
    Optional<Lsn> resumeFromLsn(Lsn currentLsn, ReplicationMessage message, Long beginMessageTransactionId);

    /**
     * Determines whether a given replication message should be skipped based on its LSN and transaction ID.
     *
     * @param lsn the LSN of the message
     * @param beginMessageTransactionId the transaction ID associated with the message
     * @return {@code true} if the message should be skipped; {@code false} otherwise
     */
    boolean skipMessage(Lsn lsn, Long beginMessageTransactionId);

    /**
     * Enables internal filtering logic that may be used to determine whether to process or skip messages.
     */
    void enableFiltering();

    /**
     * Indicates whether position searching or filtering is currently enabled.
     *
     * @return {@code true} if searching is enabled; {@code false} otherwise
     */
    boolean searchingEnabled();

    /**
     * Returns the LSN of the last event that was stored during replication processing.
     *
     * @return the last stored event's LSN, or {@code null} if none has been stored
     */
    Lsn getLastEventStoredLsn();

    /**
     * Returns the LSN of the last commit that was stored during replication processing.
     *
     * @return the last stored commit's LSN, or {@code null} if none has been stored
     */
    Lsn getLastCommitStoredLsn();
}

