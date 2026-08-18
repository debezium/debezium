/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import org.infinispan.configuration.cache.ConfigurationBuilder;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.infinispan.EmbeddedInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransactionFactory;

public class EmbeddedInfinispanFindFirstRolledBackEventEntryTest extends AbstractFindFirstRolledBackEventEntryTest<InfinispanTransaction> {
    private static final Configuration CONFIG = Configuration.create()
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS, heapOnlyCacheConfig("transactions"))
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS, heapOnlyCacheConfig("processed-transactions"))
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES, heapOnlyCacheConfig("schema-changes"))
            .with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS, heapOnlyCacheConfig("events"))
            .build();

    private static String heapOnlyCacheConfig(String cacheName) {
        return new ConfigurationBuilder().build().toStringConfiguration(cacheName);
    }

    @Override
    protected CacheProvider<InfinispanTransaction> getCacheProvider() {
        return new EmbeddedInfinispanCacheProvider(new OracleConnectorConfig(CONFIG));
    }

    @Override
    protected TransactionFactory<InfinispanTransaction> getTransactionFactory() {
        return new InfinispanTransactionFactory();
    }
}
