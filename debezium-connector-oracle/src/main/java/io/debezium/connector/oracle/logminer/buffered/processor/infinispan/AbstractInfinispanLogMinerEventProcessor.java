/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.processor.infinispan;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.buffered.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.buffered.processor.CacheProvider;
import io.debezium.connector.oracle.logminer.buffered.processor.LogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * An implementation of {@link LogMinerEventProcessor}
 * that uses Infinispan to persist the transaction cache across restarts on disk.
 *
 * @author Chris Cranford
 */
public abstract class AbstractInfinispanLogMinerEventProcessor extends AbstractLogMinerEventProcessor<InfinispanTransaction>
        implements CacheProvider<InfinispanTransaction> {

    protected AbstractInfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
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
    protected InfinispanTransaction createTransaction(LogMinerEventRow row) {
        return new InfinispanTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName(), row.getThread(), row.getClientId());
    }
}
