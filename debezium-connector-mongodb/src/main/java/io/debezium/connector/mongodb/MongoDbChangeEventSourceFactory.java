/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Optional;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
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

    private final MongoDbConnectorConfig configuration;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<MongoDbPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final ReplicaSets replicaSets;
    private final MongoDbTaskContext taskContext;
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
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<MongoDbPartition, MongoDbOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<MongoDbPartition> snapshotProgressListener) {
        return new MongoDbSnapshotChangeEventSource(
                configuration,
                taskContext,
                replicaSets,
                dispatcher,
                clock,
                snapshotProgressListener,
                errorHandler);
    }

    @Override
    public StreamingChangeEventSource<MongoDbPartition, MongoDbOffsetContext> getStreamingChangeEventSource() {
        return new MongoDbStreamingChangeEventSource(
                configuration,
                taskContext,
                replicaSets,
                dispatcher,
                errorHandler,
                clock);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<MongoDbPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                MongoDbOffsetContext offsetContext,
                                                                                                                                                SnapshotProgressListener<MongoDbPartition> snapshotProgressListener,
                                                                                                                                                DataChangeEventListener<MongoDbPartition> dataChangeEventListener) {
        final MongoDbIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource = new MongoDbIncrementalSnapshotChangeEventSource(
                configuration,
                taskContext,
                replicaSets,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
