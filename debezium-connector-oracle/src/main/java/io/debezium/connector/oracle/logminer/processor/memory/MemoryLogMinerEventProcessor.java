/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.util.HashMap;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractTransactionCachingLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerCache;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * A {@link LogMinerEventProcessor} that uses the JVM heap to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerEventProcessor extends AbstractTransactionCachingLogMinerEventProcessor<MemoryTransaction> {

    /**
     * Cache of transactions, keyed based on the transaction's unique identifier
     */
    private final LogMinerCache<String, MemoryTransaction> transactionCache = new MemoryBasedLogMinerCache<>(new HashMap<>());
    private final LogMinerCache<String, LogMinerEvent> eventCache = new MemoryBasedLogMinerCache<>(new HashMap<>());
    private final LogMinerCache<String, String> schemaCache = new MemoryBasedLogMinerCache<>(new HashMap<>());
    private final LogMinerCache<String, String> processedTransactionsCache = new MemoryBasedLogMinerCache<>(new HashMap<>());

    public MemoryLogMinerEventProcessor(ChangeEventSourceContext context,
                                        OracleConnectorConfig connectorConfig,
                                        OracleConnection jdbcConnection,
                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                        OraclePartition partition,
                                        OracleOffsetContext offsetContext,
                                        OracleDatabaseSchema schema,
                                        LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);
    }

    @Override
    protected MemoryTransaction createTransaction(LogMinerEventRow row) {
        return new MemoryTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread());
    }

    @Override
    public LogMinerCache<String, MemoryTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, LogMinerEvent> getEventCache() {

        return eventCache;
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
    }

}
