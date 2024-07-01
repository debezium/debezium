/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;

/**
 * Contract for an Oracle transaction.
 *
 * @author Chris Cranford
 */
public interface Transaction {
    /**
     * Get the transaction identifier
     *
     * @return the transaction unique identifier, never {@code null}
     */
    String getTransactionId();

    /**
     * Get the system change number of when the transaction started
     *
     * @return the system change number, never {@code null}
     */
    Scn getStartScn();

    /**
     * Get the time when the transaction started
     *
     * @return the timestamp of the transaction, never {@code null}
     */
    Instant getChangeTime();

    /**
     * Get the username associated with the transaction
     *
     * @return the username, may be {@code null}
     */
    String getUserName();

    /**
     * Get the number of events participating in the transaction.
     *
     * @return the number of transaction events
     */
    int getNumberOfEvents();

    /**
     * Helper method to get the next event identifier for the transaction.
     *
     * @return the next event identifier
     */
    int getNextEventId();

    /**
     * @return true if at least one event is a LOB type event
     */
    boolean hasLobEvent();

    /**
     * if any LOB event is captured, set flag = true
     */
    void setHasLobEvent();

    /**
     * Get the redo thread that the transaction participated on.
     *
     * @return the redo thread number
     */
    int getRedoThreadId();

    /**
     * Helper method that resets the event identifier back to {@code 0}.
     *
     * This should be called when a transaction {@code START} event is detected in the event stream.
     * This is required when LOB support is enabled to facilitate the re-mining of existing events.
     */
    void start();
}
