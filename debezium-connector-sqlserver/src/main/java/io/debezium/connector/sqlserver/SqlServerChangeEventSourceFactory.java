/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Optional;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

public class SqlServerChangeEventSourceFactory implements ChangeEventSourceFactory {

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
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new SqlServerSnapshotChangeEventSource(configuration, (SqlServerOffsetContext) offsetContext, dataConnection, schema, dispatcher, clock,
                snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new SqlServerStreamingChangeEventSource(
                configuration,
                (SqlServerOffsetContext) offsetContext,
                dataConnection,
                metadataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                              OffsetContext offsetContext,
                                                                                                                              SnapshotProgressListener snapshotProgressListener,
                                                                                                                              DataChangeEventListener dataChangeEventListener) {
        final SignalBasedIncrementalSnapshotChangeEventSource<TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<TableId>(
                configuration,
                dataConnection,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
