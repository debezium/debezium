/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metrics;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.DisconnectEvent;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Collect;

/**
 * @author Chris Cranford
 */
@ThreadSafe
public class MongoDbSnapshotChangeEventSourceMetrics extends DefaultSnapshotChangeEventSourceMetrics<MongoDbPartition>
        implements MongoDbSnapshotChangeEventSourceMetricsMBean {

    private final AtomicLong numberOfDisconnects = new AtomicLong();

    public <T extends CdcSourceTaskContext> MongoDbSnapshotChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider,
                Collect.linkMapOf("context", "snapshot", "server", taskContext.getConnectorName(), "task", taskContext.getTaskId()));
    }

    @Override
    public long getNumberOfDisconnects() {
        return numberOfDisconnects.get();
    }

    @Override
    public void onConnectorEvent(MongoDbPartition partition, ConnectorEvent event) {
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
