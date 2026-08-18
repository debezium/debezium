/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransaction;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransactionFactory;

public class EhcacheFindFirstRolledBackEventEntryTest extends AbstractFindFirstRolledBackEventEntryTest<EhcacheTransaction> {
    private static final String HEAP_ONLY_CACHE_CONFIG = "<resources><heap unit=\"entries\">100</heap></resources>";
    private static final Configuration CONFIG = Configuration.create()
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, HEAP_ONLY_CACHE_CONFIG)
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, HEAP_ONLY_CACHE_CONFIG)
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG, HEAP_ONLY_CACHE_CONFIG)
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, HEAP_ONLY_CACHE_CONFIG)
            .build();

    @Override
    protected CacheProvider<EhcacheTransaction> getCacheProvider() {
        return new EhcacheCacheProvider(new OracleConnectorConfig(CONFIG));
    }

    @Override
    protected TransactionFactory<EhcacheTransaction> getTransactionFactory() {
        return new EhcacheTransactionFactory();
    }
}
