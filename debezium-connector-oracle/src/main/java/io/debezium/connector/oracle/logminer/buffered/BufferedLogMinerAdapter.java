/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingAdapter;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * An implementation of {@link AbstractLogMinerStreamingAdapter} for capturing changes from LogMiner
 * using heap and off-heap cache/buffer mechanisms while reading LogMiner data in uncommitted mode.
 *
 * @author Chris Cranford
 */
public class BufferedLogMinerAdapter extends AbstractLogMinerStreamingAdapter {

    public static final String TYPE = "logminer";

    public BufferedLogMinerAdapter(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader() {
        return new BufferedLogMinerOracleOffsetContextLoader(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<OraclePartition, OracleOffsetContext> getSource(OracleConnection connection,
                                                                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                                      ErrorHandler errorHandler,
                                                                                      Clock clock,
                                                                                      OracleDatabaseSchema schema,
                                                                                      OracleTaskContext taskContext,
                                                                                      Configuration jdbcConfig,
                                                                                      LogMinerStreamingChangeEventSourceMetrics streamingMetrics,
                                                                                      SnapshotterService snapshotterService) {
        return new BufferedLogMinerStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                jdbcConfig,
                streamingMetrics,
                snapshotterService);
    }

    @Override
    public OracleOffsetContext copyOffset(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext) {
        return new BufferedLogMinerOracleOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }

}
