/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.LogMinerEventMarshallerImpl;
import io.debezium.connector.oracle.logminer.processor.infinispan.marshalling.TransactionMarshallerImpl;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * A concrete implementation of {@link AbstractInfinispanLogMinerEventProcessor} that uses Infinispan with
 * the Hotrod client to store transaction and mined event data in caches.
 *
 * The cache configurations are supplied via connector configurations and are expected to be valid XML
 * that represents parseable distributed cache setups for Infinispan.
 *
 * @author Chris Cranford
 */
public class RemoteInfinispanLogMinerEventProcessor extends AbstractInfinispanLogMinerEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteInfinispanLogMinerEventProcessor.class);

    private static final String HOTROD_CLIENT_LOOKUP_PREFIX = "log.mining.buffer.infinispan.client.";
    private static final String HOTROD_CLIENT_PREFIX = "infinispan.client.";

    public static final String HOTROD_SERVER_LIST = HOTROD_CLIENT_LOOKUP_PREFIX + "hotrod.server_list";

    private final RemoteCacheManager cacheManager;
    private final boolean dropBufferOnStop;

    private final RemoteCache<String, InfinispanTransaction> transactionCache;
    private final RemoteCache<String, LogMinerEvent> eventCache;
    private final RemoteCache<String, String> processedTransactionsCache;
    private final RemoteCache<String, String> schemaChangesCache;

    public RemoteInfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
                                                  OracleConnectorConfig connectorConfig,
                                                  OracleConnection jdbcConnection,
                                                  EventDispatcher<OraclePartition, TableId> dispatcher,
                                                  OraclePartition partition,
                                                  OracleOffsetContext offsetContext,
                                                  OracleDatabaseSchema schema,
                                                  LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);

        Configuration config = new ConfigurationBuilder()
                .withProperties(getHotrodClientProperties(connectorConfig))
                // todo: why must these be defined manually rather than automated like embedded mode?
                .addContextInitializer(TransactionMarshallerImpl.class.getName())
                .addContextInitializer(LogMinerEventMarshallerImpl.class.getName())
                .build();

        LOGGER.info("Using Infinispan in Hotrod client mode");
        this.cacheManager = new RemoteCacheManager(config, true);
        this.dropBufferOnStop = connectorConfig.isLogMiningBufferDropOnStop();

        this.transactionCache = createCache(TRANSACTIONS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS);
        this.processedTransactionsCache = createCache(PROCESSED_TRANSACTIONS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS);
        this.schemaChangesCache = createCache(SCHEMA_CHANGES_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES);
        this.eventCache = createCache(EVENTS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS);

        reCreateInMemoryCache();
        displayCacheStatistics();
    }

    @Override
    public void close() throws Exception {
        if (dropBufferOnStop) {
            LOGGER.info("Clearing infinispan caches");
            transactionCache.clear();
            eventCache.clear();
            schemaChangesCache.clear();
            processedTransactionsCache.clear();

            // this block should only be used by tests, should we wrap this in case admin rights aren't given?
            cacheManager.administration().removeCache(CacheProvider.TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.PROCESSED_TRANSACTIONS_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.SCHEMA_CHANGES_CACHE_NAME);
            cacheManager.administration().removeCache(CacheProvider.EVENTS_CACHE_NAME);
        }
        LOGGER.info("Shutting down infinispan remote caches");
        cacheManager.close();
    }

    @Override
    public BasicCache<String, InfinispanTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public BasicCache<String, LogMinerEvent> getEventCache() {
        return eventCache;
    }

    @Override
    public BasicCache<String, String> getSchemaChangesCache() {
        return schemaChangesCache;
    }

    @Override
    public BasicCache<String, String> getProcessedTransactionsCache() {
        return processedTransactionsCache;
    }

    @Override
    protected Scn getTransactionCacheMinimumScn() {
        Scn minimumScn = Scn.NULL;
        try (CloseableIterator<InfinispanTransaction> iterator = transactionCache.values().iterator()) {
            while (iterator.hasNext()) {
                final Scn transactionScn = iterator.next().getStartScn();
                if (minimumScn.isNull()) {
                    minimumScn = transactionScn;
                }
                else {
                    if (transactionScn.compareTo(minimumScn) < 0) {
                        minimumScn = transactionScn;
                    }
                }
            }
        }
        return minimumScn;
    }

    @Override
    protected Optional<InfinispanTransaction> getOldestTransactionInCache() {
        InfinispanTransaction transaction = null;
        if (!transactionCache.isEmpty()) {
            try (CloseableIterator<InfinispanTransaction> iterator = transactionCache.values().iterator()) {
                // Seed with the first element
                transaction = iterator.next();
                while (iterator.hasNext()) {
                    final InfinispanTransaction entry = iterator.next();
                    int comparison = entry.getStartScn().compareTo(transaction.getStartScn());
                    if (comparison < 0) {
                        // if entry has a smaller scn, it came before.
                        transaction = entry;
                    }
                    else if (comparison == 0) {
                        // if entry has an equal scn, compare the change times.
                        if (entry.getChangeTime().isBefore(transaction.getChangeTime())) {
                            transaction = entry;
                        }
                    }
                }
            }
        }
        return Optional.ofNullable(transaction);
    }

    @Override
    protected String getFirstActiveTransactionKey() {
        try (CloseableIterator<String> iterator = transactionCache.keySet().iterator()) {
            if (iterator.hasNext()) {
                return iterator.next();
            }
        }
        return null;
    }

    @Override
    protected void purgeCache(Scn minCacheScn) {
        removeIf(processedTransactionsCache.entrySet().iterator(), entry -> Scn.valueOf(entry.getValue()).compareTo(minCacheScn) < 0);
        removeIf(schemaChangesCache.entrySet().iterator(), entry -> Scn.valueOf(entry.getKey()).compareTo(minCacheScn) < 0);
    }

    private Properties getHotrodClientProperties(OracleConnectorConfig connectorConfig) {
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

        cache = cacheManager.administration().createCache(cacheName, new XMLStringConfiguration(cacheConfiguration));
        if (cache == null) {
            throw new DebeziumException("Failed to create remote Infinispan cache: " + cacheName);
        }

        LOGGER.info("Created remote infinispan cache: {}", cacheName);
        return cache;
    }
}
