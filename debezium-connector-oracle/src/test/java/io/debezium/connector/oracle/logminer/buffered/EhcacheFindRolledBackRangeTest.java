/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransaction;
import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheTransactionFactory;
import io.debezium.connector.oracle.util.TestHelper;

public class EhcacheFindRolledBackRangeTest extends AbstractFindRolledBackRangeTest<EhcacheTransaction> {

    private static final Configuration CONFIG = getConfiguration();

    @Override
    protected CacheProvider<EhcacheTransaction> getCacheProvider() {
        return new EhcacheCacheProvider(new OracleConnectorConfig(CONFIG));
    }

    @Override
    protected TransactionFactory<EhcacheTransaction> getTransactionFactory() {
        return new EhcacheTransactionFactory();
    }

    private static Configuration getConfiguration() {
        final Configuration.Builder configBuilder = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.EHCACHE.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, Boolean.TRUE);
        return TestHelper.withDefaultEhcacheConfigurations(configBuilder, 1024 * 66).build();
    }

}
