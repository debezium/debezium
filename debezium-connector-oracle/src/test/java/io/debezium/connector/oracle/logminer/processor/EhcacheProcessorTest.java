/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static org.assertj.core.api.Assertions.assertThat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.processor.ehcache.EhcacheLogMinerEventProcessor;
import io.debezium.connector.oracle.util.TestHelper;

@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Only applicable for LogMiner")
public class EhcacheProcessorTest extends AbstractProcessorUnitTest<EhcacheLogMinerEventProcessor> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheProcessorTest.class);

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.EHCACHE)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_PATH, "/ehcachedata")
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_RECENTTRANSACTIONS_SIZE_MB, 1)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_TRANSACTION_SIZE_GB, 1)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_CACHE_SCHEMACHANGES_SIZE_MB, 1)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
    }

    @Override
    protected EhcacheLogMinerEventProcessor getProcessor(OracleConnectorConfig connectorConfig) {
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();
        return new EhcacheLogMinerEventProcessor(context,
                connectorConfig,
                connection,
                dispatcher,
                partition,
                offsetContext,
                schema,
                metrics);
    }
}
