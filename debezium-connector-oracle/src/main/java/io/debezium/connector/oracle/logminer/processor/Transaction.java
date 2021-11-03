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

    int getNumberOfEvents();

    /**
     * Helper method to get the next event identifier for the transaction.
     *
     * @return the next event identifier
     */
    int getNextEventId();

    /**
     * Helper method that resets the event identifier back to {@code 0}.
     *
     * This should be called when a transaction {@code START} event is detected in the event stream.
     * This is required when LOB support is enabled to facilitate the re-mining of existing events.
     */
    void start();
}
