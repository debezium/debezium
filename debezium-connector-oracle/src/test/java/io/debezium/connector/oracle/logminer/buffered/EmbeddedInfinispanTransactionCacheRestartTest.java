/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import org.infinispan.configuration.cache.ConfigurationBuilder;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.infinispan.EmbeddedInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransactionFactory;

public class EmbeddedInfinispanTransactionCacheRestartTest extends AbstractTransactionCacheRestartTest<InfinispanTransaction> {

    @Override
    protected CacheProvider<InfinispanTransaction> createCacheProvider() {
        final Configuration config = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.INFINISPAN_EMBEDDED.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, Boolean.FALSE)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL, cacheConfig("global"))
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS, cacheConfig(CacheProvider.TRANSACTIONS_CACHE_NAME))
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS, cacheConfig(CacheProvider.EVENTS_CACHE_NAME))
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS,
                        cacheConfig(CacheProvider.PROCESSED_TRANSACTIONS_CACHE_NAME))
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES, cacheConfig(CacheProvider.SCHEMA_CHANGES_CACHE_NAME))
                .build();
        return new EmbeddedInfinispanCacheProvider(new OracleConnectorConfig(config));
    }

    @Override
    protected TransactionFactory<InfinispanTransaction> getTransactionFactory() {
        return new InfinispanTransactionFactory();
    }

    private String cacheConfig(String cacheName) {
        final String location = dataDir.resolve(cacheName).toString();
        return new ConfigurationBuilder()
                .persistence()
                .passivation(false)
                .addSoftIndexFileStore()
                .segmented(true)
                .preload(true)
                .shared(false)
                .ignoreModifications(false)
                .dataLocation(location)
                .indexLocation(location)
                .build()
                .toStringConfiguration(cacheName);
    }
}
