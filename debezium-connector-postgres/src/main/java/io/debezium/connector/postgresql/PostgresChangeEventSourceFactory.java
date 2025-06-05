/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Optional;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
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

public class PostgresChangeEventSourceFactory implements ChangeEventSourceFactory<PostgresPartition, PostgresOffsetContext> {

    private final PostgresConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final PostgresEventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresTaskContext taskContext;
    private final SnapshotterService snapshotterService;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;
    private final BeanRegistry beanRegistry;

    public PostgresChangeEventSourceFactory(PostgresConnectorConfig configuration, SnapshotterService snapshotterService,
                                            MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory,
                                            ErrorHandler errorHandler, PostgresEventDispatcher<TableId> dispatcher, Clock clock, PostgresSchema schema,
                                            PostgresTaskContext taskContext, ReplicationConnection replicationConnection, SlotCreationResult slotCreatedInfo,
                                            SlotState startingSlotInfo, BeanRegistry beanRegistry) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotterService = snapshotterService;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.beanRegistry = beanRegistry;
    }

    @Override
    public SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<PostgresPartition> snapshotProgressListener,
                                                                                                            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService,
                                                                                                            BeanRegistry beanRegistry) {
        return new PostgresSnapshotChangeEventSource(
                configuration,
                snapshotterService,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                slotCreatedInfo,
                startingSlotInfo,
                notificationService, this.beanRegistry);
    }

    @Override
    public StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> getStreamingChangeEventSource() {
        return new PostgresStreamingChangeEventSource(
                configuration,
                snapshotterService,
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
                                                                                                                                                 DataChangeEventListener<PostgresPartition> dataChangeEventListener,
                                                                                                                                                 NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
        if (configuration.isReadOnlyConnection()) {

            return Optional.of(new PostgresReadOnlyIncrementalSnapshotChangeEventSource<>(
                    configuration,
                    connectionFactory.mainConnection(),
                    dispatcher,
                    schema,
                    clock,
                    snapshotProgressListener,
                    dataChangeEventListener,
                    notificationService));
        }

        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }

        return Optional.of(new PostgresSignalBasedIncrementalSnapshotChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService));
    }
}
