/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

/**
 * @author Chris Cranford
 */
public interface CacheProvider<T extends Transaction> extends AutoCloseable {

    /**
     * The name for the transaction cache
     */
    String TRANSACTIONS_CACHE_NAME = "transactions";

    /**
     * The name for the recently processed transactions cache
     */
    String PROCESSED_TRANSACTIONS_CACHE_NAME = "processed-transactions";

    /**
     * The name for the LogMiner events cache
     */
    String EVENTS_CACHE_NAME = "events";

    /**
     * Displays cache statistics
     */
    void displayCacheStatistics();

    /**
     * Get the transaction and transaction event cache.
     *
     * @return the transaction cache, never {@code null}
     */
    LogMinerTransactionCache<T> getTransactionCache();

    /**
     * Get the processed transactions cache
     *
     * <ul>
     *     <li>Key - The unique transaction id</li>
     *     <li>Value - The transaction's commit or rollback system change number</li>
     * </ul>
     *
     * @return the processed transactions cache, never {@code null}
     */
    LogMinerCache<String, String> getProcessedTransactionsCache();
}
