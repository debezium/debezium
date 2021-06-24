/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class OracleChangeEventSourceFactory implements ChangeEventSourceFactory<OracleOffsetContext> {

    private final OracleConnectorConfig configuration;
    private final OracleConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final Configuration jdbcConfig;
    private final OracleTaskContext taskContext;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;

    public OracleChangeEventSourceFactory(OracleConnectorConfig configuration, OracleConnection jdbcConnection,
                                          ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, OracleDatabaseSchema schema,
                                          Configuration jdbcConfig, OracleTaskContext taskContext,
                                          OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.taskContext = taskContext;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<OracleOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new OracleSnapshotChangeEventSource(configuration, jdbcConnection,
                schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<OracleOffsetContext> getStreamingChangeEventSource() {
        return configuration.getAdapter().getSource(
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                jdbcConfig,
                streamingMetrics);
    }
}
