/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.List;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransactionFactory;
import io.debezium.connector.oracle.logminer.buffered.infinispan.RemoteInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * Round trip of {@link LogMinerRedoEvent} through a remote Infinispan server; runs only when the build selects the
 * remote buffer, which the {@code oracle-infinispan-buffer-remote} profile does while starting the server.
 *
 * @author Chris Cranford
 */
public class RemoteInfinispanLogMinerRedoEventRoundTripIT extends AbstractLogMinerEventRoundTripTest<InfinispanTransaction> {

    private static final Configuration CONFIG = getConfiguration();

    @BeforeAll
    static void requiresRemoteBuffer() {
        final String bufferType = System.getProperty(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE.name());
        Assumptions.assumeTrue(LogMiningBufferType.INFINISPAN_REMOTE.getValue().equalsIgnoreCase(bufferType),
                "Requires -Dlog.mining.buffer.type=infinispan_remote and a running Infinispan server");
    }

    @Override
    protected CacheProvider<InfinispanTransaction> getCacheProvider() {
        return new RemoteInfinispanCacheProvider(new OracleConnectorConfig(CONFIG));
    }

    @Override
    protected TransactionFactory<InfinispanTransaction> getTransactionFactory() {
        return new InfinispanTransactionFactory();
    }

    @Override
    protected List<LogMinerRedoEvent> sampleEvents() {
        return LogMinerRedoEventFixtures.allEvents();
    }

    @Override
    protected void assertSameEvent(LogMinerRedoEvent expected, LogMinerRedoEvent actual) {
        LogMinerRedoEventFixtures.assertSameEvent(expected, actual);
    }

    private static Configuration getConfiguration() {
        final LogMiningBufferType bufferType = LogMiningBufferType.INFINISPAN_REMOTE;
        final Configuration.Builder configBuilder = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, bufferType.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, Boolean.TRUE)
                .with("log.mining.buffer." + ConfigurationProperties.SERVER_LIST, TestHelper.INFINISPAN_SERVER_LIST)
                .with("log.mining.buffer." + ConfigurationProperties.AUTH_USERNAME, TestHelper.INFINISPAN_USER)
                .with("log.mining.buffer." + ConfigurationProperties.AUTH_PASSWORD, TestHelper.INFINISPAN_PASS);
        return TestHelper.withDefaultInfinispanCacheConfigurations(bufferType, configBuilder).build();
    }
}
