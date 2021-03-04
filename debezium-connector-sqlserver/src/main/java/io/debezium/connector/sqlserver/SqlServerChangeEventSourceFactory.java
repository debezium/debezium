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
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class SqlServerChangeEventSourceFactory implements ChangeEventSourceFactory<SqlServerTaskPartition, SqlServerOffsetContext> {

    private final SqlServerConnectorConfig configuration;
    private final SqlServerConnection dataConnection;
    private final SqlServerConnection metadataConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;

    public SqlServerChangeEventSourceFactory(SqlServerConnectorConfig configuration, SqlServerConnection dataConnection, SqlServerConnection metadataConnection,
                                             ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, SqlServerDatabaseSchema schema) {
        this.configuration = configuration;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<SqlServerTaskPartition, SqlServerOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new SqlServerSnapshotChangeEventSource(configuration, dataConnection, schema, dispatcher, clock,
                snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<SqlServerTaskPartition, SqlServerOffsetContext> getStreamingChangeEventSource() {
        return new SqlServerStreamingChangeEventSource(
                configuration,
                dataConnection,
                metadataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }
}
