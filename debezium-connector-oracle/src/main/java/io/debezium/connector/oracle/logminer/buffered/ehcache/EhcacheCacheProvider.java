/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG;
import static io.debezium.connector.oracle.OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.xml.XmlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.AbstractCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.LogMinerCache;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * Provides access to various transaction-focused caches to store transaction details in Ehcache
 * while processing change events from Oracle LogMiner's buffered implementation.
 *
 * @author Chris Cranford
 */
public class EhcacheCacheProvider extends AbstractCacheProvider<EhcacheTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheCacheProvider.class);

    private static final String FEATURE_DISALLOW_DOCTYPE = "http://apache.org/xml/features/disallow-doctype-decl";
    private static final String FEATURE_EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
    private static final String FEATURE_EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
    private static final String JDK_DOCUMENT_BUILDER_FACTORY_IMPL = "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl";

    private final boolean dropBufferOnStop;
    private final CacheManager cacheManager;
    private final EhcacheLogMinerTransactionCache transactionCache;
    private final EhcacheLogMinerCache<String, String> processedTransactionsCache;

    public EhcacheCacheProvider(OracleConnectorConfig connectorConfig) {
        LOGGER.info("Using Ehcache provider to buffer transactions");

        this.dropBufferOnStop = connectorConfig.isLogMiningBufferDropOnStop();
        this.cacheManager = createCacheManager(connectorConfig);

        final EhcacheEvictionListener evictionListener = new EhcacheEvictionListener();

        this.transactionCache = createTransactionCache(evictionListener, connectorConfig.getLogMiningBufferGrowthMax());
        this.processedTransactionsCache = createProcessedTransactionCache(evictionListener);
        clearRetainedState();
    }

    @Override
    public LogMinerTransactionCache<EhcacheTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, String> getProcessedTransactionsCache() {
        return processedTransactionsCache;
    }

    @Override
    public void close() throws Exception {
        final boolean wasInterrupted = Thread.interrupted();
        try {
            if (cacheManager != null) {
                if (dropBufferOnStop) {
                    LOGGER.info("Clearing Ehcache caches");
                    transactionCache.clear();
                    processedTransactionsCache.clear();

                    cacheManager.removeCache(TRANSACTIONS_CACHE_NAME);
                    cacheManager.removeCache(PROCESSED_TRANSACTIONS_CACHE_NAME);
                    cacheManager.removeCache(EVENTS_CACHE_NAME);
                }

                LOGGER.info("Shutting down Ehcache embedded caches");
                cacheManager.close();
            }
        }
        finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private CacheManager createCacheManager(OracleConnectorConfig connectorConfig) {
        try {
            final Configuration ehcacheConfig = connectorConfig.getLogMiningEhcacheConfiguration();

            // Create the full XML configuration based on configuration template
            // Explicitly use the JDK built-in parser to avoid Oracle's JXDocumentBuilderFactory
            // being picked up via service-loader when Oracle XML JARs are on the classpath,
            // as Oracle's parser does not support the Apache XXE security features below.
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance(
                    JDK_DOCUMENT_BUILDER_FACTORY_IMPL,
                    EhcacheCacheProvider.class.getClassLoader());

            // Required for propagating namespace info
            factory.setNamespaceAware(true);

            enableSecurityFeatures(factory);

            final DocumentBuilder builder = factory.newDocumentBuilder();

            final String xmlData = getConfigurationWithSubstitutions(ehcacheConfig);
            LOGGER.debug("Using Ehcache XML configuration:\n{}", xmlData);

            final Document xmlDocument = builder.parse(new InputSource(new StringReader(xmlData)));

            final XmlConfiguration xmlConfiguration = new XmlConfiguration(xmlDocument);
            final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(xmlConfiguration);
            try {
                cacheManager.init();
                return cacheManager;
            }
            catch (StateTransitionException e) {
                if (!isPersistedEventTypeMismatch(e) || !(cacheManager instanceof PersistentCacheManager persistentCacheManager)) {
                    throw e;
                }
                // Ehcache refuses to open a persistent cache whose stored value type differs from the configured one,
                // which happens when a buffer left behind was written by a version that stored parsed events. The buffer
                // is transient and would be cleared anyway, so it is destroyed as a whole and recreated.
                LOGGER.warn("The Ehcache transaction buffer left by a previous run was written by a version that stored parsed events; destroying it.");
                persistentCacheManager.destroy();
                final CacheManager replacement = CacheManagerBuilder.newCacheManager(xmlConfiguration);
                replacement.init();
                return replacement;
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to create Ehcache cache manager", e);
        }
    }

    private static boolean isPersistedEventTypeMismatch(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof IllegalArgumentException && cause.getMessage() != null
                    && cause.getMessage().startsWith("Persisted value type")) {
                return true;
            }
        }
        return false;
    }

    private void enableSecurityFeatures(DocumentBuilderFactory factory) throws ParserConfigurationException {
        factory.setFeature(FEATURE_DISALLOW_DOCTYPE, true);
        factory.setFeature(FEATURE_EXTERNAL_GENERAL_ENTITIES, false);
        factory.setFeature(FEATURE_EXTERNAL_PARAMETER_ENTITIES, false);
        factory.setExpandEntityReferences(false);
    }

    private String getConfigurationWithSubstitutions(Configuration configuration) {
        return readConfigurationTemplate()
                .replace("${log.mining.buffer.ehcache.global.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.transactions.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.processedtransactions.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, ""))
                .replace("${log.mining.buffer.ehcache.events.config}",
                        configuration.getString(LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, ""));
    }

    private String readConfigurationTemplate() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("ehcache/configuration-template.xml")) {
            if (inputStream == null) {
                throw new DebeziumException("Failed to find ehcache/configuration-template.xml");
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to read Ehcache configuration template", e);
        }
    }

    private <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType, EhcacheEvictionListener listener) {
        final Cache<K, V> cache = cacheManager.getCache(cacheName, keyType, valueType);
        cache.getRuntimeConfiguration().registerCacheEventListener(listener, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, Set.of(EventType.EVICTED));
        return cache;
    }

    private EhcacheLogMinerTransactionCache createTransactionCache(EhcacheEvictionListener evictionListener, int growthMax) {
        return new EhcacheLogMinerTransactionCache(
                getCache(TRANSACTIONS_CACHE_NAME, String.class, EhcacheTransaction.class, evictionListener),
                getCache(EVENTS_CACHE_NAME, String.class, LogMinerRedoEvent.class, evictionListener),
                evictionListener,
                growthMax);
    }

    private EhcacheLogMinerCache<String, String> createProcessedTransactionCache(EhcacheEvictionListener evictionListener) {
        return new EhcacheLogMinerCache<>(
                getCache(PROCESSED_TRANSACTIONS_CACHE_NAME, String.class, String.class, evictionListener),
                PROCESSED_TRANSACTIONS_CACHE_NAME,
                evictionListener);
    }
}
