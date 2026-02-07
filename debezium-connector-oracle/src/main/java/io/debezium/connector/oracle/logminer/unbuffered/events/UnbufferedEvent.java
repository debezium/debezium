/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered.events;

/**
 * Common contract for all unbuffered-specific events.
 *
 * @author Chris Cranford
 */
public interface UnbufferedEvent {
    /**
     * Get the transaction identifier associated with the event.
     * @return the transaction identifier
     */
    String getTransactionId();

    /**
     * Get the transaction sequence associated with the event.
     * @return the transaction sequence
     */
    Long getTransactionSequence();
}
