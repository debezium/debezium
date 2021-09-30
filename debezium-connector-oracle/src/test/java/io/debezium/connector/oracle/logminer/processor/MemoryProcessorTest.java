/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Only applicable for LogMiner")
public class MemoryProcessorTest extends AbstractProcessorUnitTest<MemoryLogMinerEventProcessor> {
    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, "memory")
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
    }

    @Override
    protected MemoryLogMinerEventProcessor getProcessor(OracleConnectorConfig connectorConfig) {
        return new MemoryLogMinerEventProcessor(context,
                connectorConfig,
                connection,
                dispatcher,
                partition,
                offsetContext,
                schema,
                metrics);
    }
}
