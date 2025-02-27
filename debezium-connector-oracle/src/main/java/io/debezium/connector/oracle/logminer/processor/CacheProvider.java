/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

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
     * The name for the schema changes cache
     */
    String SCHEMA_CHANGES_CACHE_NAME = "schema-changes";

    /**
     * The name for the LogMiner events cache
     */
    String EVENTS_CACHE_NAME = "events";

    /**
     * Displays cache statistics
     */
    void displayCacheStatistics();

    /**
     * Get the transaction cache
     *
     * <ul>
     *     <li>Key - The unique transaction id</li>
     *     <li>Value - The transaction instance</li>
     * </ul>
     *
     * @return the transaction cache, never {@code null}
     */
    LogMinerCache<String, T> getTransactionCache();

    /**
     * Get the LogMiner events cache
     *
     * <ul>
     *     <li>Key - The event id, in the format of {@code transactionId-eventSequence}</li>
     *     <li>Value - The raw LogMinerEvent object instance</li>
     * </ul>
     *
     * @return the evnts cache, never {@code null}
     */
    LogMinerCache<String, LogMinerEvent> getEventCache();

    /**
     * Get the Schema Changes cache
     *
     * <ul>
     *     <li>Key - The system change number of the schema change</li>
     *     <li>Value - The table the schema change is related to</li>
     * </ul>
     *
     * @return the schema changes cache, never {@code null}
     */
    LogMinerCache<String, String> getSchemaChangesCache();

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
