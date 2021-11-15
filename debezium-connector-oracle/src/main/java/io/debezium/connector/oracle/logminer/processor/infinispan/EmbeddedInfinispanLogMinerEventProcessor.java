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

import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * A concrete implementation of {@link AbstractInfinispanLogMinerEventProcessor} that uses Infinispan in
 * embedded mode to store transaction and mined event data in caches.
 *
 * The cache configurations are supplied via connector configurations and are expected to be valid XML
 * that represent parseable local cache setups for Infinispan.
 *
 * @author Chris Cranford
 */
public class EmbeddedInfinispanLogMinerEventProcessor extends AbstractInfinispanLogMinerEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedInfinispanLogMinerEventProcessor.class);

    private final EmbeddedCacheManager cacheManager;
    private final boolean dropBufferOnStop;

    private final Cache<String, InfinispanTransaction> transactionCache;
    private final Cache<String, LogMinerEvent> eventCache;
    private final Cache<String, String> processedTransactionsCache;
    private final Cache<String, String> schemaChangesCache;

    public EmbeddedInfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
                                                    OracleConnectorConfig connectorConfig,
                                                    OracleConnection jdbcConnection,
                                                    EventDispatcher<TableId> dispatcher,
                                                    OraclePartition partition,
                                                    OracleOffsetContext offsetContext,
                                                    OracleDatabaseSchema schema,
                                                    OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);

        LOGGER.info("Using Infinispan in embedded mode.");
        this.cacheManager = new DefaultCacheManager();
        this.dropBufferOnStop = connectorConfig.isLogMiningBufferDropOnStop();

        this.transactionCache = createCache(TRANSACTIONS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS);
        this.processedTransactionsCache = createCache(PROCESSED_TRANSACTIONS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS);
        this.schemaChangesCache = createCache(SCHEMA_CHANGES_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES);
        this.eventCache = createCache(EVENTS_CACHE_NAME, connectorConfig, LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS);

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
        LOGGER.info("Shutting down infinispan embedded caches");
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

    private <K, V> Cache<K, V> createCache(String cacheName, OracleConnectorConfig connectorConfig, Field field) {
        Objects.requireNonNull(cacheName);

        final String cacheConfiguration = connectorConfig.getConfig().getString(field);
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
