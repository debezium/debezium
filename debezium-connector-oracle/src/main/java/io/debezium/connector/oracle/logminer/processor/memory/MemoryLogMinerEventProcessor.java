/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerCache;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerTransactionCache;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * A {@link LogMinerEventProcessor} that uses the JVM heap to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerEventProcessor extends AbstractLogMinerEventProcessor<MemoryTransaction> {

    private final LogMinerTransactionCache<MemoryTransaction> transactionCache = new MemoryLogMinerTransactionCache();
    private final LogMinerCache<String, String> schemaCache = new MemoryBasedLogMinerCache<>();
    private final LogMinerCache<String, String> processedTransactionsCache = new MemoryBasedLogMinerCache<>();

    public MemoryLogMinerEventProcessor(ChangeEventSourceContext context,
                                        OracleConnectorConfig connectorConfig,
                                        OracleConnection jdbcConnection,
                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                        OraclePartition partition,
                                        OracleOffsetContext offsetContext,
                                        OracleDatabaseSchema schema,
                                        LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics, jdbcConnection);
    }

    @Override
    protected MemoryTransaction createTransaction(LogMinerEventRow row) {
        return new MemoryTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    public LogMinerTransactionCache<MemoryTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, String> getSchemaChangesCache() {
        return schemaCache;
    }

    @Override
    public LogMinerCache<String, String> getProcessedTransactionsCache() {
        return processedTransactionsCache;
    }

    @Override
    public void close() throws Exception {
        transactionCache.close();
    }

}
