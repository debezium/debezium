/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.logminer.buffered.infinispan.EmbeddedInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransactionFactory;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * The events cache stores ProtoStream bytes, so every read goes through
 * {@link io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling.LogMinerRedoEventAdapter}, as with
 * a remote server.
 *
 * @author Chris Cranford
 */
public class EmbeddedInfinispanLogMinerRedoEventRoundTripTest extends AbstractLogMinerEventRoundTripTest<InfinispanTransaction> {

    private static final Configuration CONFIG = getConfiguration();

    @Override
    protected CacheProvider<InfinispanTransaction> getCacheProvider() {
        return new EmbeddedInfinispanCacheProvider(new OracleConnectorConfig(CONFIG));
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
        final LogMiningBufferType bufferType = LogMiningBufferType.INFINISPAN_EMBEDDED;
        final Configuration.Builder configBuilder = Configuration.create()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, bufferType.getValue())
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, Boolean.TRUE);

        TestHelper.withDefaultInfinispanCacheConfigurations(bufferType, configBuilder);

        configBuilder.with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS,
                new ConfigurationBuilder()
                        .encoding()
                        .mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
                        .persistence()
                        .passivation(false)
                        .addSoftIndexFileStore()
                        .segmented(true)
                        .preload(true)
                        .shared(false)
                        .ignoreModifications(false)
                        .dataLocation("./target/data")
                        .indexLocation("./target/data")
                        .build()
                        .toStringConfiguration(CacheProvider.EVENTS_CACHE_NAME));

        return configBuilder.build();
    }
}
