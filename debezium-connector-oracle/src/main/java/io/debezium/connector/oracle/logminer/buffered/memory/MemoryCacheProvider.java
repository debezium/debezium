/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.AbstractCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.LogMinerCache;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;

/**
 * Provides access to various transaction-focused caches to store transaction details in memory
 * while processing change events from Oracle LogMiner's buffered implementation.
 *
 * @author Chris Cranford
 */
public class MemoryCacheProvider extends AbstractCacheProvider<MemoryTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryCacheProvider.class);

    private final MemoryLogMinerTransactionCache transactionCache;
    private final MemoryBasedLogMinerCache<String, String> processedTransactionsCache;

    public MemoryCacheProvider(OracleConnectorConfig connectorConfig) {
        LOGGER.info("Using Java heap to buffer transactions (chunk size {} bytes, compression {}, growth cap {} events)",
                connectorConfig.getLogMiningBufferMemoryChunkSize(),
                connectorConfig.isLogMiningBufferMemoryCompression() ? "on" : "off",
                connectorConfig.getLogMiningBufferGrowthMax());

        this.transactionCache = new MemoryLogMinerTransactionCache(
                connectorConfig.getLogMiningBufferMemoryChunkSize(),
                connectorConfig.getLogMiningBufferGrowthMax(),
                connectorConfig.isLogMiningBufferMemoryCompression());
        this.processedTransactionsCache = new MemoryBasedLogMinerCache<>();
    }

    @Override
    public LogMinerTransactionCache<MemoryTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, String> getProcessedTransactionsCache() {
        return processedTransactionsCache;
    }

    @Override
    public void close() throws Exception {
    }

}
