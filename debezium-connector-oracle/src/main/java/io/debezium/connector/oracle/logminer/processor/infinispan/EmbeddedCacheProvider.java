/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_COMMITTED_TRANSACTIONS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACK_TRANSACTIONS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS;

import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * @author Chris Cranford
 */
public class EmbeddedCacheProvider implements CacheProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedCacheProvider.class);

    private final EmbeddedCacheManager cacheManager;
    private final boolean dropBufferOnStop;

    private final Cache<String, InfinispanTransaction> transactionCache;
    private final Cache<String, LogMinerEvent> eventCache;
    private final Cache<String, String> recentlyCommittedTransactionsCache;
    private final Cache<String, String> rollbackTransactionsCache;
    private final Cache<String, String> schemaChangesCache;

    public EmbeddedCacheProvider(OracleConnectorConfig connectorConfig) {
        this.cacheManager = new DefaultCacheManager();
        this.dropBufferOnStop = connectorConfig.isLogMiningBufferDropOnStop();

        final String transactionConfig = connectorConfig.getConfig().getString(LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS);
        this.transactionCache = createCache(TRANSACTIONS_CACHE_NAME, transactionConfig);

        final String committedTransactionsConfig = connectorConfig.getConfig().getString(LOG_MINING_BUFFER_INFINISPAN_CACHE_COMMITTED_TRANSACTIONS);
        this.recentlyCommittedTransactionsCache = createCache(COMMIT_TRANSACTIONS_CACHE_NAME, committedTransactionsConfig);

        final String rollbackTransactionsConfig = connectorConfig.getConfig().getString(LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACK_TRANSACTIONS);
        this.rollbackTransactionsCache = createCache(ROLLBACK_TRANSACTIONS_CACHE_NAME, rollbackTransactionsConfig);

        final String schemaChangesConfig = connectorConfig.getConfig().getString(LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES);
        this.schemaChangesCache = createCache(SCHEMA_CHANGES_CACHE_NAME, schemaChangesConfig);

        final String eventsConfig = connectorConfig.getConfig().getString(LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS);
        this.eventCache = createCache(EVENTS_CACHE_NAME, eventsConfig);
    }

    @Override
    public Cache<String, InfinispanTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public Cache<String, LogMinerEvent> getEventCache() {
        return eventCache;
    }

    @Override
    public Cache<String, String> getSchemaChangesCache() {
        return schemaChangesCache;
    }

    @Override
    public Cache<String, String> getCommittedTransactionsCache() {
        return recentlyCommittedTransactionsCache;
    }

    @Override
    public Cache<String, String> getRollbackedTransactionsCache() {
        return rollbackTransactionsCache;
    }

    @Override
    public void close() throws Exception {
        if (dropBufferOnStop) {
            LOGGER.info("Clearing infinispan caches");
            transactionCache.clear();
            eventCache.clear();
            schemaChangesCache.clear();
            recentlyCommittedTransactionsCache.clear();
            rollbackTransactionsCache.clear();

            // this block should only be used by tests, should we wrap this in case admin rights aren't given?
            cacheManager.administration().removeCache(CacheProvider.TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.COMMIT_TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.ROLLBACK_TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.SCHEMA_CHANGES_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.EVENTS_CACHE_NAME);
        }
        LOGGER.info("Shutting down infinispan embedded caches");
        cacheManager.close();
    }

    @Override
    public void displayCacheStatistics() {
        LOGGER.info("Cache Statistics:");
        LOGGER.info("\tTransactions   : {}", transactionCache.size());
        LOGGER.info("\tCommitted Trxs : {}", recentlyCommittedTransactionsCache.size());
        LOGGER.info("\tRollback Trxs  : {}", rollbackTransactionsCache.size());
        LOGGER.info("\tSchema Changes : {}", schemaChangesCache.size());
        LOGGER.info("\tEvents         : {}", eventCache.size());
        if (!eventCache.isEmpty()) {
            for (String eventKey : eventCache.keySet()) {
                LOGGER.debug("\t\tFound Key: {}", eventKey);
            }
        }
    }

    private <K, V> Cache<K, V> createCache(String cacheName, String cacheConfiguration) {
        Objects.requireNonNull(cacheName);
        Objects.requireNonNull(cacheConfiguration);

        // define the cache, parsing the supplied XML configuration
        cacheManager.defineConfiguration(cacheName,
                new ParserRegistry().parse(cacheConfiguration)
                        .getNamedConfigurationBuilders()
                        .get(cacheName)
                        .build());

        return cacheManager.getCache(cacheName);
    }
}
