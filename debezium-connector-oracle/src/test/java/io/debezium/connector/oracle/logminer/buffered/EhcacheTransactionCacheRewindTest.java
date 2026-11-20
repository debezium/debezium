/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransaction;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransactionFactory;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

public class EhcacheTransactionCacheRewindTest extends AbstractTransactionCacheRewindTest<EhcacheTransaction> {

    // A dedicated directory, as Ehcache keeps persistent spaces on disk after the caches are closed
    private static final Path PERSISTENCE_PATH = Paths.get("target", "data", "ehcache-rewind-test");

    @BeforeAll
    static void beforeAll() {
        Testing.Files.delete(PERSISTENCE_PATH);
    }

    @AfterAll
    static void afterAll() {
        Testing.Files.delete(PERSISTENCE_PATH);
    }

    @Override
    protected CacheProvider<EhcacheTransaction> getCacheProvider(boolean dropOnStop) {
        return new EhcacheCacheProvider(new OracleConnectorConfig(getConfiguration(dropOnStop)));
    }

    @Override
    protected boolean isBufferPersisted() {
        return true;
    }

    @Override
    protected TransactionFactory<EhcacheTransaction> getTransactionFactory() {
        return new EhcacheTransactionFactory();
    }

    private static Configuration getConfiguration(boolean dropOnStop) {
        final Configuration.Builder configBuilder = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.EHCACHE.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, dropOnStop);
        TestHelper.withDefaultEhcacheConfigurations(configBuilder, 1024 * 66);
        // A persistent disk tier, so that the buffer survives a restart
        final String cacheConfig = "<resources><heap unit=\"entries\">512</heap><disk unit=\"B\" persistent=\"true\">67584</disk></resources>";
        return configBuilder
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, "<persistence directory=\"" + PERSISTENCE_PATH + "\"/>")
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, cacheConfig)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, cacheConfig)
                .build();
    }

}
