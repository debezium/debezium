/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransaction;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransactionFactory;
import io.debezium.util.IoUtil;

/**
 * @author Chris Cranford
 */
public class EhcacheStoredEventFormatTest extends AbstractStoredEventFormatTest<EhcacheTransaction> {

    // A persistent tier outlives its cache manager, so this test keeps its own directory, away from the
    // non-persistent tiers the other tests open under target/data, and removes it after each test
    private static final Path DATA_DIRECTORY = Paths.get("target", "data-stored-event-format");

    @Override
    protected CacheProvider<EhcacheTransaction> provider(boolean dropOnStop) {
        return new EhcacheCacheProvider(new OracleConnectorConfig(getConfiguration(dropOnStop)));
    }

    @Override
    protected TransactionFactory<EhcacheTransaction> getTransactionFactory() {
        return new EhcacheTransactionFactory();
    }

    @Override
    protected void deleteRetainedState() throws Exception {
        IoUtil.delete(DATA_DIRECTORY);
    }

    private static Configuration getConfiguration(boolean dropOnStop) {
        // A disk tier only survives a cache manager restart when it is declared persistent
        final String cacheConfig = "<resources><heap unit=\"entries\">512</heap><disk unit=\"B\" persistent=\"true\">" + (1024 * 1024) + "</disk></resources>";
        return Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.EHCACHE.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, dropOnStop)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, "<persistence directory=\"" + DATA_DIRECTORY + "\"/>")
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, cacheConfig)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, cacheConfig)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, cacheConfig)
                .build();
    }
}
