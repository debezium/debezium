/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Optional;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.mongodb.metrics.MongoDbStreamingChangeEventSourceMetrics;
import io.debezium.connector.mongodb.snapshot.MongoDbIncrementalSnapshotChangeEventSource;
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
import io.debezium.snapshot.SnapshotterService;
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
    private final MongoDbTaskContext taskContext;
    private final MongoDbSchema schema;
    private final MongoDbStreamingChangeEventSourceMetrics streamingMetrics;
    private final SnapshotterService snapshotterService;
    private final BeanRegistry beanRegistry;

    public MongoDbChangeEventSourceFactory(MongoDbConnectorConfig configuration, ErrorHandler errorHandler,
                                           EventDispatcher<MongoDbPartition, CollectionId> dispatcher, Clock clock,
                                           MongoDbTaskContext taskContext, MongoDbSchema schema,
                                           MongoDbStreamingChangeEventSourceMetrics streamingMetrics, SnapshotterService snapshotterService, BeanRegistry beanRegistry) {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.taskContext = taskContext;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
        this.snapshotterService = snapshotterService;
        this.beanRegistry = beanRegistry;
    }

    @Override
    public SnapshotChangeEventSource<MongoDbPartition, MongoDbOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<MongoDbPartition> snapshotProgressListener,
                                                                                                          NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService,
                                                                                                          BeanRegistry beanRegistry) {
        return new MongoDbSnapshotChangeEventSource(
                configuration,
                taskContext,
                dispatcher,
                clock,
                snapshotProgressListener,
                errorHandler,
                notificationService,
                snapshotterService, this.beanRegistry);
    }

    @Override
    public StreamingChangeEventSource<MongoDbPartition, MongoDbOffsetContext> getStreamingChangeEventSource() {
        return new MongoDbStreamingChangeEventSource(
                configuration,
                taskContext,
                dispatcher,
                errorHandler,
                clock,
                streamingMetrics,
                snapshotterService);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<MongoDbPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                MongoDbOffsetContext offsetContext,
                                                                                                                                                SnapshotProgressListener<MongoDbPartition> snapshotProgressListener,
                                                                                                                                                DataChangeEventListener<MongoDbPartition> dataChangeEventListener,
                                                                                                                                                NotificationService<MongoDbPartition, MongoDbOffsetContext> notificationService) {
        final MongoDbIncrementalSnapshotChangeEventSource incrementalSnapshotChangeEventSource = new MongoDbIncrementalSnapshotChangeEventSource(
                configuration,
                taskContext,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
