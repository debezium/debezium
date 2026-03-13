/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

/**
 * Provides a view of a transaction's current state for spill strategy decisions.
 *
 * @author Debezium Authors
 */
public interface TransactionView {

    /**
     * Gets the number of events currently stored in memory for this transaction.
     *
     * @return the count of in-memory events
     */
    long memoryEventCount();

    /**
     * Gets the total number of events for this transaction (memory + spilled).
     *
     * @return the total count of events
     */
    long totalEventCount();
}
