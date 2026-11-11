/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransaction;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransactionFactory;

public class EhcacheTransactionCacheRestartTest extends AbstractTransactionCacheRestartTest<EhcacheTransaction> {

    private static final String CACHE_CONFIG = "<resources>" +
            "<heap unit=\"entries\">512</heap>" +
            "<disk unit=\"MB\" persistent=\"true\">64</disk>" +
            "</resources>";

    @Override
    protected CacheProvider<EhcacheTransaction> createCacheProvider() {
        final Configuration config = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.EHCACHE.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, Boolean.FALSE)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, "<persistence directory=\"" + dataDir + "\"/>")
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, CACHE_CONFIG)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, CACHE_CONFIG)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, CACHE_CONFIG)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG, CACHE_CONFIG)
                .build();
        return new EhcacheCacheProvider(new OracleConnectorConfig(config));
    }

    @Override
    protected TransactionFactory<EhcacheTransaction> getTransactionFactory() {
        return new EhcacheTransactionFactory();
    }
}
