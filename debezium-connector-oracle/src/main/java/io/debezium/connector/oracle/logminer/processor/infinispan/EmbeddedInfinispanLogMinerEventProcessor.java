/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.Map;

import org.infinispan.commons.util.CloseableIterator;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class EmbeddedInfinispanLogMinerEventProcessor extends AbstractInfinispanLogMinerEventProcessor {

    private final EmbeddedCacheProvider cacheProvider;

    public EmbeddedInfinispanLogMinerEventProcessor(ChangeEventSourceContext context,
                                                    OracleConnectorConfig connectorConfig,
                                                    OracleConnection jdbcConnection,
                                                    EventDispatcher<TableId> dispatcher,
                                                    OraclePartition partition,
                                                    OracleOffsetContext offsetContext,
                                                    OracleDatabaseSchema schema,
                                                    OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);

        cacheProvider = new EmbeddedCacheProvider(connectorConfig);
        cacheProvider.displayCacheStatistics();
    }

    @Override
    public void close() throws Exception {
        cacheProvider.close();
    }

    @Override
    protected Map<String, InfinispanTransaction> getTransactionCache() {
        return cacheProvider.getTransactionCache();
    }

    @Override
    protected EmbeddedCacheProvider getCacheProvider() {
        return cacheProvider;
    }

    @Override
    protected Scn getTransactionCacheMinimumScn() {
        Scn minimumScn = Scn.NULL;
        try (CloseableIterator<InfinispanTransaction> iterator = cacheProvider.getTransactionCache().values().iterator()) {
            while (iterator.hasNext()) {
                final Scn transactionScn = iterator.next().getStartScn();
                if (minimumScn.isNull()) {
                    minimumScn = transactionScn;
                }
                else {
                    if (transactionScn.compareTo(minimumScn) < 0) {
                        minimumScn = transactionScn;
                    }
                }
            }
        }
        return minimumScn;
    }
}
