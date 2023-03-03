/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Optional;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class PostgresChangeEventSourceFactory implements ChangeEventSourceFactory<PostgresPartition, PostgresOffsetContext> {

    private final PostgresConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final PostgresEventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public PostgresChangeEventSourceFactory(PostgresConnectorConfig configuration, Snapshotter snapshotter, MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                            ErrorHandler errorHandler, PostgresEventDispatcher<TableId> dispatcher, Clock clock, PostgresSchema schema,
                                            PostgresTaskContext taskContext, ReplicationConnection replicationConnection, SlotCreationResult slotCreatedInfo,
                                            SlotState startingSlotInfo) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
    }

    @Override
    public SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<PostgresPartition> snapshotProgressListener) {
        return new PostgresSnapshotChangeEventSource(
                configuration,
                snapshotter,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                slotCreatedInfo,
                startingSlotInfo);
    }

    @Override
    public StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> getStreamingChangeEventSource() {
        return new PostgresStreamingChangeEventSource(
                configuration,
                snapshotter,
                connectionFactory.mainConnection(),
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<PostgresPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                 PostgresOffsetContext offsetContext,
                                                                                                                                                 SnapshotProgressListener<PostgresPartition> snapshotProgressListener,
                                                                                                                                                 DataChangeEventListener<PostgresPartition> dataChangeEventListener) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }
        final PostgresSignalBasedIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource = new PostgresSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
