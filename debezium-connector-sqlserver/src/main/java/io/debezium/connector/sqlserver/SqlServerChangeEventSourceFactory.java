/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class SqlServerChangeEventSourceFactory implements ChangeEventSourceFactory<SqlServerPartition, SqlServerOffsetContext> {

    private final SqlServerConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory;
    private final SqlServerConnection metadataConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<SqlServerPartition, TableId> dispatcher;
    private final Clock clock;
    private final SqlServerDatabaseSchema schema;
    private final NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService;
    private final SnapshotterService snapshotterService;

    public SqlServerChangeEventSourceFactory(SqlServerConnectorConfig configuration, MainConnectionProvidingConnectionFactory<SqlServerConnection> connectionFactory,
                                             SqlServerConnection metadataConnection, ErrorHandler errorHandler, EventDispatcher<SqlServerPartition, TableId> dispatcher,
                                             Clock clock, SqlServerDatabaseSchema schema,
                                             NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService, SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.metadataConnection = metadataConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.notificationService = notificationService;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<SqlServerPartition, SqlServerOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                                                                              NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService) {
        return new SqlServerSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<SqlServerPartition, SqlServerOffsetContext> getStreamingChangeEventSource() {
        return new SqlServerStreamingChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                metadataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                notificationService,
                snapshotterService);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<SqlServerPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                  SqlServerOffsetContext offsetContext,
                                                                                                                                                  SnapshotProgressListener<SqlServerPartition> snapshotProgressListener,
                                                                                                                                                  DataChangeEventListener<SqlServerPartition> dataChangeEventListener,
                                                                                                                                                  NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        final SignalBasedIncrementalSnapshotChangeEventSource<SqlServerPartition, TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
