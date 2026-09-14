/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
@SkipWhenDatabaseVersion(major = 21, check = EqualityCheck.EQUAL, reason = "Oracle 21 shared memory pool fragments and crashes")
public class MemoryStreamingChangeEventSourceIT extends AbstractBufferedLogMinerStreamingChangeEventSourceIT {
    @Override
    protected Configuration.Builder getBufferImplementationConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.MEMORY);
    }
}
