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

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.LogMinerEventMarshallerImpl;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.TransactionMarshallerImpl;

/**
 * @author Chris Cranford
 */
public class RemoteCacheProvider implements CacheProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteCacheProvider.class);

    private final RemoteCacheManager cacheManager;
    private final boolean dropBufferOnStop;

    private final RemoteCache<String, InfinispanTransaction> transactionCache;
    private final RemoteCache<String, LogMinerEvent> eventCache;
    private final RemoteCache<String, String> recentlyCommittedTransactionsCache;
    private final RemoteCache<String, String> rollbackTransactionsCache;
    private final RemoteCache<String, String> schemaChangesCache;

    public RemoteCacheProvider(OracleConnectorConfig connectorConfig) {
        Configuration config = new ConfigurationBuilder()
                .withProperties(getHotrodClientProperties(connectorConfig))
                // todo: why must these be defined manually rather than automated like embedded mode?
                .addContextInitializer(TransactionMarshallerImpl.class.getName())
                .addContextInitializer(LogMinerEventMarshallerImpl.class.getName())
                .build();

        this.cacheManager = new RemoteCacheManager(config, true);
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

    private Properties getHotrodClientProperties(OracleConnectorConfig connectorConfig) {
        final Map<String, String> clientSettings = connectorConfig.getConfig()
                .subset("log.mining.buffer.infinispan.client.", true)
                .asMap();

        final Properties properties = new Properties();
        for (Map.Entry<String, String> entry : clientSettings.entrySet()) {
            properties.put("infinispan.client." + entry.getKey(), entry.getValue());
        }
        return properties;
    }

    @Override
    public RemoteCache<String, InfinispanTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public RemoteCache<String, LogMinerEvent> getEventCache() {
        return eventCache;
    }

    @Override
    public RemoteCache<String, String> getSchemaChangesCache() {
        return schemaChangesCache;
    }

    @Override
    public RemoteCache<String, String> getCommittedTransactionsCache() {
        return recentlyCommittedTransactionsCache;
    }

    @Override
    public RemoteCache<String, String> getRollbackedTransactionsCache() {
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
        LOGGER.info("Shutting down infinispan remote caches");
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

    private <C, V> RemoteCache<C, V> createCache(String cacheName, String cacheConfiguration) {
        Objects.requireNonNull(cacheName);
        Objects.requireNonNull(cacheConfiguration);

        RemoteCache<C, V> cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            // cache is already defined, simply return it
            LOGGER.info("Remote cache '{}' already defined.", cacheName);
            return cache;
        }

        // In ISPN 12.1, configuration can only be supplied as XML.
        cache = cacheManager.administration().createCache(cacheName, new XMLStringConfiguration(cacheConfiguration));
        if (cache == null) {
            throw new DebeziumException("Failed to create remote Infinispan cache: " + cacheName);
        }

        LOGGER.info("Created remote infinispan cache: {}", cacheName);
        return cache;
    }
}
