/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransactionFactory;

public class MemoryFindFirstRolledBackEventEntryTest extends AbstractFindFirstRolledBackEventEntryTest<MemoryTransaction> {
    @Override
    protected CacheProvider<MemoryTransaction> getCacheProvider() {
        return new MemoryCacheProvider(new OracleConnectorConfig(Configuration.empty()));
    }

    @Override
    protected TransactionFactory<MemoryTransaction> getTransactionFactory() {
        return new MemoryTransactionFactory();
    }
}
