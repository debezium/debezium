/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

public class SqlServerChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final SqlServerConnectorConfig configuration;
    private final SqlServerConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher dispatcher;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;

    public SqlServerChangeEventSourceFactory(SqlServerConnectorConfig configuration, SqlServerConnection jdbcConnection,
            ErrorHandler errorHandler, EventDispatcher dispatcher, Clock clock, SqlServerDatabaseSchema schema) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext) {
        return new SqlServerSnapshotChangeEventSource(configuration, (SqlServerOffsetContext) offsetContext, jdbcConnection, schema);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new SqlServerStreamingChangeEventSource(
                configuration,
                (SqlServerOffsetContext) offsetContext,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema
        );
    }
}
