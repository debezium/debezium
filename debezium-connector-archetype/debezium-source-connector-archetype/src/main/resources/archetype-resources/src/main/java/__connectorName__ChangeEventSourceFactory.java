/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Creates the snapshot and streaming change event sources for the ${connectorName} connector.
 *
 * <p>Called once per connector start by {@link io.debezium.pipeline.ChangeEventSourceCoordinator}.
 */
class ${connectorName}ChangeEventSourceFactory
        implements ChangeEventSourceFactory<${connectorName}Partition, ${connectorName}OffsetContext> {

    private final ${connectorName}ConnectorConfig config;
    private final MainConnectionProvidingConnectionFactory<${connectorName}Connection> connectionFactory;
    private final ${connectorName}DatabaseSchema schema;
    private final TableId dataCollectionId;
    private final EventDispatcher<${connectorName}Partition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final SnapshotterService snapshotterService;

    ${connectorName}ChangeEventSourceFactory(${connectorName}ConnectorConfig config,
                                              MainConnectionProvidingConnectionFactory<${connectorName}Connection> connectionFactory,
                                              ${connectorName}DatabaseSchema schema,
                                              TableId dataCollectionId,
                                              EventDispatcher<${connectorName}Partition, TableId> dispatcher,
                                              ErrorHandler errorHandler,
                                              Clock clock,
                                              SnapshotterService snapshotterService) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.schema = schema;
        this.dataCollectionId = dataCollectionId;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<${connectorName}Partition> progressListener,
            NotificationService<${connectorName}Partition, ${connectorName}OffsetContext> notificationService) {
        return new ${connectorName}SnapshotChangeEventSource(config, connectionFactory, schema, dispatcher, clock,
                progressListener, notificationService, snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> getStreamingChangeEventSource() {
        return new ${connectorName}StreamingChangeEventSource(config, dataCollectionId, dispatcher, errorHandler, clock);
    }
}
