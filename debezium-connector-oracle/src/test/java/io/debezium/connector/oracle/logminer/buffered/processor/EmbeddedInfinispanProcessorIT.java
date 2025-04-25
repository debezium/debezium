/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor;

import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

/**
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
public class EmbeddedInfinispanProcessorIT extends AbstractProcessorTest {
    @Before
    public void before() throws Exception {
        super.before();

        // Before each test make sure to clear all infinispan caches
        Testing.Files.delete(Testing.Files.createTestingPath("data/transactions.dat").toAbsolutePath());
        Testing.Files.delete(Testing.Files.createTestingPath("data/committed-transactions.dat").toAbsolutePath());
        Testing.Files.delete(Testing.Files.createTestingPath("data/rollback-transactions.dat").toAbsolutePath());
        Testing.Files.delete(Testing.Files.createTestingPath("data/schema-changes.dat").toAbsolutePath());
    }

    @Override
    protected Configuration.Builder getBufferImplementationConfig() {
        final LogMiningBufferType bufferType = LogMiningBufferType.INFINISPAN_EMBEDDED;
        return TestHelper.withDefaultInfinispanCacheConfigurations(bufferType,
                TestHelper.defaultConfig()
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, bufferType)
                        .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true));
    }

    @Override
    protected boolean hasPersistedState() {
        return true;
    }
}
