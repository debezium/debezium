/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import org.infinispan.commons.api.BasicCache;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * @author Chris Cranford
 */
public interface CacheProvider extends AutoCloseable {

    /**
     * The name for the transaction cache
     */
    String TRANSACTIONS_CACHE_NAME = "transactions";

    /**
     * The name for the recently committed transactions cache
     */
    String COMMIT_TRANSACTIONS_CACHE_NAME = "committed-transactions";

    /**
     * The name for the rollback transactions cache
     */
    String ROLLBACK_TRANSACTIONS_CACHE_NAME = "rollback-transactions";

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
    BasicCache<String, InfinispanTransaction> getTransactionCache();

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
    BasicCache<String, LogMinerEvent> getEventCache();

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
    BasicCache<String, String> getSchemaChangesCache();

    /**
     * Get the recently committed transactions cache
     *
     * <ul>
     *     <li>Key - The unique transaction id</li>
     *     <li>Value - The transaction's commit system change number</li>
     * </ul>
     *
     * @return the committed transactions cache, never {@code null}
     */
    BasicCache<String, String> getCommittedTransactionsCache();

    /**
     * Get the rollback transactions cache.
     *
     * <ul>
     *     <li>Key - The unique transaction id</li>
     *     <li>Value - The transaction's rollback system change number</li>
     * </ul>
     *
     * @return the rollback transactions cache, never {@code null}
     */
    BasicCache<String, String> getRollbackTransactionsCache();
}
