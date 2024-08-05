/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache;

import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractTransactionCachingLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerCache;
import io.debezium.connector.oracle.logminer.processor.ehcache.serialization.EhcacheTransactionSerializer;
import io.debezium.connector.oracle.logminer.processor.ehcache.serialization.LogMinerEventSerializer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An {@link AbstractTransactionCachingLogMinerEventProcessor} implementation for storing buffer details
 * off-heap in a set of Ehcache-backed caches.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerEventProcessor extends AbstractTransactionCachingLogMinerEventProcessor<EhcacheTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheLogMinerEventProcessor.class);

    private final PersistentCacheManager cacheManager;
    private final LogMinerCache<String, EhcacheTransaction> transactionsCache;
    private final LogMinerCache<String, LogMinerEvent> eventCache;
    private final LogMinerCache<String, String> processedTransactionsCache;
    private final LogMinerCache<String, String> schemaChangesCache;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public EhcacheLogMinerEventProcessor(ChangeEventSourceContext context,
                                         OracleConnectorConfig connectorConfig,
                                         OracleConnection jdbcConnection,
                                         EventDispatcher<OraclePartition, TableId> dispatcher,
                                         OraclePartition partition,
                                         OracleOffsetContext offsetContext,
                                         OracleDatabaseSchema schema,
                                         LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);
        LOGGER.info("Using Ehcache buffer");

        this.cacheManager = createCacheManager(connectorConfig);
        this.transactionsCache = new EhcacheLogMinerCache(cacheManager.getCache(TRANSACTIONS_CACHE_NAME, String.class, EhcacheTransaction.class));
        this.processedTransactionsCache = new EhcacheLogMinerCache(cacheManager.getCache(PROCESSED_TRANSACTIONS_CACHE_NAME, String.class, String.class));
        this.schemaChangesCache = new EhcacheLogMinerCache(cacheManager.getCache(SCHEMA_CHANGES_CACHE_NAME, String.class, String.class));
        this.eventCache = new EhcacheLogMinerCache(cacheManager.getCache(EVENTS_CACHE_NAME, String.class, LogMinerEvent.class));
    }

    @Override
    protected EhcacheTransaction createTransaction(LogMinerEventRow row) {
        return new EhcacheTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    public LogMinerCache<String, EhcacheTransaction> getTransactionCache() {
        return transactionsCache;
    }

    @Override
    public LogMinerCache<String, LogMinerEvent> getEventCache() {
        return eventCache;
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
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    private PersistentCacheManager createCacheManager(OracleConnectorConfig connectorConfig) {
        final String cachePath = connectorConfig.getLogMiningBufferEhcacheStoragePath();
        final long transactionCacheSize = connectorConfig.getLogMiningBufferEhcacheTransactionCacheSizeBytes();
        final long processedTransactionsCacheSize = connectorConfig.getLogMiningBufferEhcacheProcessedTransactionsCacheSizeBytes();
        final long schemaChangesCacheSize = connectorConfig.getLogMiningBufferEhcacheSchemaChangesCacheSizeBytes();
        final long eventsCacheSize = connectorConfig.getLogMiningBufferEhcacheEventsCacheSizeBytes();

        return CacheManagerBuilder.newCacheManagerBuilder()
                .with(CacheManagerBuilder.persistence(cachePath))
                .using(new DefaultStatisticsService())
                .withCache(TRANSACTIONS_CACHE_NAME, createCacheConfiguration(String.class, EhcacheTransaction.class, transactionCacheSize, null))
                .withCache(PROCESSED_TRANSACTIONS_CACHE_NAME, createCacheConfiguration(String.class, String.class, processedTransactionsCacheSize, null))
                .withCache(SCHEMA_CHANGES_CACHE_NAME, createCacheConfiguration(String.class, String.class, schemaChangesCacheSize, null))
                .withCache(EVENTS_CACHE_NAME, createCacheConfiguration(String.class, LogMinerEvent.class, eventsCacheSize, null))
                .withSerializer(EhcacheTransaction.class, EhcacheTransactionSerializer.class)
                .withSerializer(LogMinerEvent.class, LogMinerEventSerializer.class)
                .build(true);
    }

    @SuppressWarnings("SameParameterValue")
    private <K, V> CacheConfiguration<K, V> createCacheConfiguration(Class<K> keyClass, Class<V> valueClass, long sizeMb,
                                                                     Class<? extends Serializer<V>> valueSerializer) {
        final CacheConfigurationBuilder<K, V> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder(
                keyClass,
                valueClass,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                        .disk(sizeMb, MemoryUnit.B, !getConfig().isLogMiningBufferDropOnStop()))
                .withExpiry(ExpiryPolicy.NO_EXPIRY);

        if (valueSerializer != null) {
            builder.withValueSerializer(valueSerializer);
        }

        return builder.build();
    }
}
