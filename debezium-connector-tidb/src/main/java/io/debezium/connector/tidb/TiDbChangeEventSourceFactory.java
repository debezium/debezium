/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Factory for the change event sources of the TiDB connector.
 *
 * @author Aviral Srivastava
 */
public class TiDbChangeEventSourceFactory implements ChangeEventSourceFactory<TiDbPartition, TiDbOffsetContext> {

    private final TiDbConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TiDbPartition, TableId> dispatcher;
    private final Clock clock;
    private final TiDbSchema schema;

    public TiDbChangeEventSourceFactory(TiDbConnectorConfig connectorConfig, ErrorHandler errorHandler,
                                        EventDispatcher<TiDbPartition, TableId> dispatcher, Clock clock, TiDbSchema schema) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<TiDbPartition, TiDbOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<TiDbPartition> snapshotProgressListener,
                                                                                                    NotificationService<TiDbPartition, TiDbOffsetContext> notificationService) {
        return new TiDbSnapshotChangeEventSource(connectorConfig);
    }

    @Override
    public StreamingChangeEventSource<TiDbPartition, TiDbOffsetContext> getStreamingChangeEventSource() {
        return new TiCdcStreamingChangeEventSource(connectorConfig, dispatcher, errorHandler, clock, schema);
    }
}
