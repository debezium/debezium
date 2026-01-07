/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.StringConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.AbstractCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.LogMinerCache;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling.LogMinerEventMarshallerImpl;
import io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling.TransactionMarshallerImpl;

/**
 * Provides access to various transaction-focused caches to store transaction details in Infinispan
 * using the Hotrod client with a remote server while processing change events for Oracle LogMiner's
 * buffered implementation.
 *
 * @author Chris Cranford
 */
public class RemoteInfinispanCacheProvider extends AbstractCacheProvider<InfinispanTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteInfinispanCacheProvider.class);

    private static final String HOTROD_CLIENT_LOOKUP_PREFIX = "log.mining.buffer.infinispan.client.";
    private static final String HOTROD_CLIENT_PREFIX = "infinispan.client.";

    public static final String HOTROD_SERVER_LIST = HOTROD_CLIENT_LOOKUP_PREFIX + "hotrod.server_list";

    private final boolean dropBufferOnStop;
    private final RemoteCacheManager cacheManager;
    private final LogMinerTransactionCache<InfinispanTransaction> transactionCache;
    private final InfinispanLogMinerCache<String, String> processedTransactionsCache;
    private final InfinispanLogMinerCache<String, String> schemaChangesCache;

    public RemoteInfinispanCacheProvider(OracleConnectorConfig connectorConfig) {
        LOGGER.info("Using Infinispan in Hotrod client mode to buffer transactions");

        this.dropBufferOnStop = connectorConfig.isLogMiningBufferDropOnStop();
        this.cacheManager = new RemoteCacheManager(createClientConfig(connectorConfig), true);

        this.transactionCache = createTransactionCache(connectorConfig);
        this.processedTransactionsCache = createProcessedTransactionCache(connectorConfig);
        this.schemaChangesCache = createSchemaChangesCache(connectorConfig);

        displayCacheStatistics();
    }

    @Override
    public LogMinerTransactionCache<InfinispanTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, String> getSchemaChangesCache() {
        return schemaChangesCache;
    }

    @Override
    public LogMinerCache<String, String> getProcessedTransactionsCache() {
        return processedTransactionsCache;
    }

    @Override
    public void close() throws Exception {
        if (dropBufferOnStop) {
            LOGGER.info("Clearing infinispan caches");
            transactionCache.clear();
            schemaChangesCache.clear();
            processedTransactionsCache.clear();

            // this block should only be used by tests, should we wrap this in case admin rights aren't given?
            cacheManager.administration().removeCache(TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(PROCESSED_TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(SCHEMA_CHANGES_CACHE_NAME);
            cacheManager.administration().removeCache(EVENTS_CACHE_NAME);
        }
        LOGGER.info("Shutting down infinispan remote caches");
        cacheManager.close();
    }

    private <C, V> RemoteCache<C, V> createCache(String cacheName, OracleConnectorConfig connectorConfig, Field field) {
        Objects.requireNonNull(cacheName);

        RemoteCache<C, V> cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            // cache is already defined, simply return it
            LOGGER.info("Remote cache '{}' already defined.", cacheName);
            return cache;
        }

        final String cacheConfiguration = connectorConfig.getConfig().getString(field);
        Objects.requireNonNull(cacheConfiguration);

        cache = cacheManager.administration().createCache(cacheName, new StringConfiguration(cacheConfiguration));
        if (cache == null) {
            throw new DebeziumException("Failed to create remote Infinispan cache: " + cacheName);
        }

        LOGGER.info("Created remote infinispan cache: {}", cacheName);
        return cache;
    }

    private InfinispanLogMinerTransactionCache createTransactionCache(OracleConnectorConfig connectorConfig) {
        return new InfinispanLogMinerTransactionCache(
                createCache(TRANSACTIONS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS),
                createCache(EVENTS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS));
    }

    private InfinispanLogMinerCache<String, String> createProcessedTransactionCache(OracleConnectorConfig connectorConfig) {
        return new InfinispanLogMinerCache<>(
                createCache(PROCESSED_TRANSACTIONS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS));
    }

    private InfinispanLogMinerCache<String, String> createSchemaChangesCache(OracleConnectorConfig connectorConfig) {
        return new InfinispanLogMinerCache<>(
                createCache(SCHEMA_CHANGES_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES));
    }

    private static Configuration createClientConfig(OracleConnectorConfig connectorConfig) {
        return new ConfigurationBuilder()
                .withProperties(getHotrodClientProperties(connectorConfig))
                // todo: why must these be defined manually rather than automated like embedded mode?
                .addContextInitializer(TransactionMarshallerImpl.class.getName())
                .addContextInitializer(LogMinerEventMarshallerImpl.class.getName())
                .build();
    }

    private static Properties getHotrodClientProperties(OracleConnectorConfig connectorConfig) {
        final Map<String, String> clientSettings = connectorConfig.getConfig()
                .subset(HOTROD_CLIENT_LOOKUP_PREFIX, true)
                .asMap();

        final Properties properties = new Properties();
        for (Map.Entry<String, String> entry : clientSettings.entrySet()) {
            properties.put(HOTROD_CLIENT_PREFIX + entry.getKey(), entry.getValue());
            if (entry.getKey().toLowerCase().endsWith(ConfigurationProperties.AUTH_USERNAME.toLowerCase())) {
                // If an authentication username is supplied, enforce authentication required
                properties.put(ConfigurationProperties.USE_AUTH, "true");
            }
        }
        return properties;
    }
}
