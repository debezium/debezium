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

    // todo: consolidate with processor most likely?
    // with the embedded/remote impls of the infinispan processors, there is really not
    // much reason to have the separate provider contracts

    String TRANSACTIONS_CACHE_NAME = "transactions";
    String COMMIT_TRANSACTIONS_CACHE_NAME = "committed-transactions";
    String ROLLBACK_TRANSACTIONS_CACHE_NAME = "rollback-transactions";
    String SCHEMA_CHANGES_CACHE_NAME = "schema-changes";
    String EVENTS_CACHE_NAME = "events";

    void displayCacheStatistics();

    BasicCache<String, InfinispanTransaction> getTransactionCache();

    BasicCache<String, LogMinerEvent> getEventCache();

    BasicCache<String, String> getSchemaChangesCache();

    BasicCache<String, String> getCommittedTransactionsCache();

    BasicCache<String, String> getRollbackedTransactionsCache();
}
