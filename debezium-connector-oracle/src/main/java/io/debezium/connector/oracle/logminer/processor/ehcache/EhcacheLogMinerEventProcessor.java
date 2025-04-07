/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.CacheProvider;
import io.debezium.connector.oracle.logminer.processor.LogMinerCache;
import io.debezium.connector.oracle.logminer.processor.LogMinerTransactionCache;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An {@link AbstractLogMinerEventProcessor} implementation for storing buffer details off-heap in a
 * set of Ehcache-backed caches.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerEventProcessor extends AbstractLogMinerEventProcessor<EhcacheTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheLogMinerEventProcessor.class);

    private final CacheManager cacheManager;
    private final EhcacheLogMinerTransactionCache transactionCache;
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
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics, jdbcConnection);
        LOGGER.info("Using Ehcache buffer");

        this.cacheManager = createCacheManager(connectorConfig);

        this.transactionCache = new EhcacheLogMinerTransactionCache(
                cacheManager.getCache(TRANSACTIONS_CACHE_NAME, String.class, EhcacheTransaction.class),
                cacheManager.getCache(EVENTS_CACHE_NAME, String.class, LogMinerEvent.class));
        this.processedTransactionsCache = new EhcacheLogMinerCache(
                cacheManager.getCache(PROCESSED_TRANSACTIONS_CACHE_NAME, String.class, String.class));
        this.schemaChangesCache = new EhcacheLogMinerCache(
                cacheManager.getCache(SCHEMA_CHANGES_CACHE_NAME, String.class, String.class));
    }

    @Override
    protected EhcacheTransaction createTransaction(LogMinerEventRow row) {
        return new EhcacheTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    public LogMinerTransactionCache<EhcacheTransaction> getTransactionCache() {
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
        if (cacheManager != null) {
            if (getConfig().isLogMiningBufferDropOnStop()) {
                LOGGER.info("Clearing Ehcache caches");
                transactionCache.clear();
                schemaChangesCache.clear();
                processedTransactionsCache.clear();

                cacheManager.removeCache(CacheProvider.TRANSACTIONS_CACHE_NAME);
                cacheManager.removeCache(CacheProvider.PROCESSED_TRANSACTIONS_CACHE_NAME);
                cacheManager.removeCache(CacheProvider.SCHEMA_CHANGES_CACHE_NAME);
                cacheManager.removeCache(CacheProvider.EVENTS_CACHE_NAME);
            }

            LOGGER.info("Shutting down Ehcache embedded caches");
            cacheManager.close();
        }
    }

    private CacheManager createCacheManager(OracleConnectorConfig connectorConfig) {
        try {
            final Configuration ehcacheConfig = connectorConfig.getLogMiningEhcacheConfiguration();

            // Create the full XML configuration based on configuration template
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            // Required for propagating namespace info
            factory.setNamespaceAware(true);

            final DocumentBuilder builder = factory.newDocumentBuilder();

            final String xmlData = getConfigurationWithSubstitutions(ehcacheConfig);
            LOGGER.debug("Using Ehcache XML configuration:\n{}", xmlData);

            final Document xmlDocument = builder.parse(new InputSource(new StringReader(xmlData)));

            final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(new XmlConfiguration(xmlDocument));
            cacheManager.init();

            return cacheManager;
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to create Ehcache cache manager", e);
        }
    }

    private String getConfigurationWithSubstitutions(Configuration configuration) {
        return readConfigurationTemplate()
                .replace("${log.mining.buffer.ehcache.global.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.transactions.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.processedtransactions.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.schemachanges.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.events.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, ""));
    }

    private String readConfigurationTemplate() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("ehcache/configuration-template.xml")) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(inputStream));
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
            finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to read Ehcache configuration template", e);
        }
    }
}
