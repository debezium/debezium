/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class OracleChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final OracleConnectorConfig configuration;
    private final OracleConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final Configuration config;
    private final OracleTaskContext taskContext;

    public OracleChangeEventSourceFactory(OracleConnectorConfig configuration, OracleConnection jdbcConnection,
                                          ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, OracleDatabaseSchema schema,
                                          Configuration config, OracleTaskContext taskContext) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.config = config;
        this.taskContext = taskContext;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new OracleSnapshotChangeEventSource(configuration, (OracleOffsetContext) offsetContext, jdbcConnection,
                schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        OracleConnectorConfig.ConnectorAdapter adapter = configuration.getAdapter();
        if (adapter == OracleConnectorConfig.ConnectorAdapter.XSTREAM) {
            return new XstreamStreamingChangeEventSource(
                    configuration,
                    (OracleOffsetContext) offsetContext,
                    jdbcConnection,
                    dispatcher,
                    errorHandler,
                    clock,
                    schema);
        }
        return new LogMinerStreamingChangeEventSource(
                configuration,
                (OracleOffsetContext) offsetContext,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext);
    }
}
