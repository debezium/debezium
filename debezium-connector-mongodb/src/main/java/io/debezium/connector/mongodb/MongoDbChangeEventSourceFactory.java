/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.connection.MongoDbConnection.AUTHORIZATION_FAILURE_MESSAGE;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Factory for creating {@link ChangeEventSource}s specific for the MongoDb connector.
 *
 * @author Chris Cranford
 */
public class MongoDbChangeEventSourceFactory implements ChangeEventSourceFactory<MongoDbPartition, MongoDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnection.class);

    private final MongoDbConnectorConfig configuration;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final ReplicaSets replicaSets;
    private final MongoDbTaskContext taskContext;
    private final MongoDbConnection.ChangeEventSourceConnectionFactory connections;
    private final MongoDbSchema schema;

    public MongoDbChangeEventSourceFactory(MongoDbConnectorConfig configuration, ErrorHandler errorHandler,
                                           EventDispatcher<MongoDbPartition, CollectionId> dispatcher, Clock clock,
                                           ReplicaSets replicaSets, MongoDbTaskContext taskContext, MongoDbSchema schema) {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.replicaSets = replicaSets;
        this.taskContext = taskContext;
        this.connections = getMongoDbConnectionFactory(taskContext.getConnectionContext());
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<MongoDbPartition, MongoDbOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<MongoDbPartition> snapshotProgressListener,
                                                                                                          NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService) {
        return new MongoDbSnapshotChangeEventSource(
                configuration,
                taskContext,
                connections,
                replicaSets,
                dispatcher,
                clock,
                snapshotProgressListener,
                errorHandler,
                notificationService);
    }

    @Override
    public StreamingChangeEventSource<MongoDbPartition, MongoDbOffsetContext> getStreamingChangeEventSource() {
        return new MongoDbStreamingChangeEventSource(
                configuration,
                taskContext,
                connections,
                replicaSets,
                dispatcher,
                errorHandler,
                clock);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<MongoDbPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                MongoDbOffsetContext offsetContext,
                                                                                                                                                SnapshotProgressListener<MongoDbPartition> snapshotProgressListener,
                                                                                                                                                DataChangeEventListener<MongoDbPartition> dataChangeEventListener,
                                                                                                                                                NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService) {
        if (replicaSets.size() > 1) {
            LOGGER.info("Only ReplicaSet deployments and Sharded Cluster with connection.mode=sharded are supported by incremental snapshot");
            return Optional.empty();
        }

        final MongoDbIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource = new MongoDbIncrementalSnapshotChangeEventSource(
                configuration,
                connections,
                replicaSets,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }

    public MongoDbConnection.ChangeEventSourceConnectionFactory getMongoDbConnectionFactory(ConnectionContext connectionContext) {
        return (ReplicaSet replicaSet, MongoDbPartition partition) -> connectionContext.connect(
                replicaSet, taskContext.filters(), connectionErrorHandler(partition));
    }

    private MongoDbConnection.ErrorHandler connectionErrorHandler(MongoDbPartition partition) {
        return (String desc, Throwable error) -> {
            if (error.getMessage() == null || !error.getMessage().startsWith(AUTHORIZATION_FAILURE_MESSAGE)) {
                dispatcher.dispatchConnectorEvent(partition, new DisconnectEvent());
            }

            LOGGER.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
            throw new DebeziumException("Error while attempting to " + desc, error);
        };
    }
}
