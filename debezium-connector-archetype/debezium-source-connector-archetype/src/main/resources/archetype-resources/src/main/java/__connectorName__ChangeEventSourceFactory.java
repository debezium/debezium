/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;

/**
 * Creates the snapshot and streaming change event sources for the ${connectorName} connector.
 *
 * <p>Called once per connector start by {@link io.debezium.pipeline.ChangeEventSourceCoordinator}.
 */
class ${connectorName}ChangeEventSourceFactory
        implements ChangeEventSourceFactory<${connectorName}Partition, ${connectorName}OffsetContext> {

    private final ${connectorName}ConnectorConfig config;
    private final ${connectorName}DataCollectionId dataCollectionId;
    private final EventDispatcher<${connectorName}Partition, ${connectorName}DataCollectionId> dispatcher;
    private final ErrorHandler errorHandler;

    ${connectorName}ChangeEventSourceFactory(${connectorName}ConnectorConfig config,
                                              ${connectorName}DataCollectionId dataCollectionId,
                                              EventDispatcher<${connectorName}Partition, ${connectorName}DataCollectionId> dispatcher,
                                              ErrorHandler errorHandler) {
        this.config = config;
        this.dataCollectionId = dataCollectionId;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
    }

    @Override
    public SnapshotChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<${connectorName}Partition> progressListener,
            NotificationService<${connectorName}Partition, ${connectorName}OffsetContext> notificationService) {
        return new ${connectorName}SnapshotChangeEventSource(config, dataCollectionId, dispatcher, progressListener);
    }

    @Override
    public StreamingChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> getStreamingChangeEventSource() {
        return new ${connectorName}StreamingChangeEventSource(config, dataCollectionId, dispatcher, errorHandler);
    }
}
