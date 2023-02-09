/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class SqlServerChangeEventSourceFactory implements ChangeEventSourceFactory<SqlServerPartition, SqlServerOffsetContext> {

    private final SqlServerConnectorConfig configuration;
    private final MainConnectionFactory<SqlServerConnection> connectionFactory;
    private final SqlServerConnection metadataConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<SqlServerPartition, TableId> dispatcher;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;

    public SqlServerChangeEventSourceFactory(SqlServerConnectorConfig configuration, MainConnectionFactory<SqlServerConnection> connectionFactory,
                                             SqlServerConnection metadataConnection, ErrorHandler errorHandler, EventDispatcher<SqlServerPartition, TableId> dispatcher,
                                             Clock clock, SqlServerDatabaseSchema schema) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.metadataConnection = metadataConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<SqlServerPartition> snapshotProgressListener) {
        return new SqlServerSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<SqlServerPartition, SqlServerOffsetContext> getStreamingChangeEventSource() {
        return new SqlServerStreamingChangeEventSource(
                configuration,
                connectionFactory.getMainConnection(),
                metadataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<SqlServerPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                  SqlServerOffsetContext offsetContext,
                                                                                                                                                  SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                                                                                                                  DataChangeEventListener<SqlServerPartition> dataChangeEventListener) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        final SignalBasedIncrementalSnapshotChangeEventSource<SqlServerPartition, TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.getMainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
