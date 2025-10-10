/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * Interface for determining when a transaction should begin spilling events to an off-heap provider.
 * This allows for pluggable strategies to decide spill behavior based on different criteria.
 *
 * @author Debezium Authors
 */
public interface SpillStrategy {

    /**
     * Determines if a transaction should begin spilling events to the spillover provider.
     *
     * @param txId the transaction ID
     * @param latest the latest event being added to the transaction
     * @param view a view of the transaction's current state
     * @return true if the transaction should begin spilling, false otherwise
     */
    boolean shouldSpill(String txId, LogMinerEvent latest, TransactionView view);

    /**
     * Called when a new transaction is created. This allows the strategy to initialize
     * any per-transaction state or tracking required for spill decisions.
     * The default implementation does nothing.
     *
     * @param txId the transaction ID of the newly created transaction
     */
    default void onTransactionCreated(String txId) {
    }

    /**
     * Called when a transaction is removed from the cache. This allows the strategy to
     * clean up any per-transaction state or tracking that was maintained for spill decisions.
     * The default implementation does nothing.
     *
     * @param txId the transaction ID of the removed transaction
     */
    default void onTransactionRemoved(String txId) {
    }
}
