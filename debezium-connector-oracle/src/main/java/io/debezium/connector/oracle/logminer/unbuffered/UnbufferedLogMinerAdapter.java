/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import io.debezium.common.annotation.Incubating;
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
 * An Oracle LogMiner {@link io.debezium.connector.oracle.StreamingAdapter} implementation that relies on
 * Oracle LogMiner's {@code COMMITTED_DATA_ONLY} mode to capture changes without requiring that the
 * connector buffer large transactions.
 *
 * @author Chris Cranford
 */
@Incubating
public class UnbufferedLogMinerAdapter extends AbstractLogMinerStreamingAdapter {

    public static final String TYPE = "logminer_unbuffered";

    public UnbufferedLogMinerAdapter(OracleConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public String getType() {
        return TYPE;
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
        return new UnbufferedLogMinerStreamingChangeEventSource(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                jdbcConfig,
                streamingMetrics);
    }

    @Override
    public OffsetContext.Loader<OracleOffsetContext> getOffsetContextLoader() {
        return new UnbufferedLogMinerOracleOffsetContextLoader(connectorConfig);
    }

    @Override
    public OracleOffsetContext copyOffset(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext) {
        return new UnbufferedLogMinerOracleOffsetContextLoader(connectorConfig).load(offsetContext.getOffset());
    }
}
