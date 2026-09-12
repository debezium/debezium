/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.infinispan.EmbeddedInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransactionFactory;
import io.debezium.connector.oracle.util.TestHelper;

public class EmbeddedInfinispanFindRolledBackRangeTest extends AbstractFindRolledBackRangeTest<InfinispanTransaction> {

    private static final Configuration CONFIG = getConfiguration();

    @Override
    protected CacheProvider<InfinispanTransaction> getCacheProvider() {
        return new EmbeddedInfinispanCacheProvider(new OracleConnectorConfig(CONFIG));
    }

    @Override
    protected TransactionFactory<InfinispanTransaction> getTransactionFactory() {
        return new InfinispanTransactionFactory();
    }

    private static Configuration getConfiguration() {
        final LogMiningBufferType bufferType = LogMiningBufferType.INFINISPAN_EMBEDDED;
        final Configuration.Builder configBuilder = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, bufferType.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, Boolean.TRUE);

        return TestHelper.withDefaultInfinispanCacheConfigurations(bufferType, configBuilder).build();
    }
}
