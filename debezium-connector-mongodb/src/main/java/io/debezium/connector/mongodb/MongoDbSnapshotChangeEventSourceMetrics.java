/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class MongoDbSnapshotChangeEventSourceMetrics extends SnapshotChangeEventSourceMetrics implements MongoDbSnapshotChangeEventSourceMetricsMBean {

    private AtomicLong numberOfDisconnects = new AtomicLong();

    public <T extends CdcSourceTaskContext> MongoDbSnapshotChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public long getNumberOfDisconnects() {
        return numberOfDisconnects.get();
    }

    @Override
    public void onConnectorEvent(ConnectorEvent event) {
        if (event instanceof DisconnectEvent) {
            numberOfDisconnects.incrementAndGet();
        }
    }

    @Override
    public void reset() {
        super.reset();
        numberOfDisconnects.set(0);
    }
}
