/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.MetadataEvent;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class MongoDbStreamingChangeEventSourceMetrics extends StreamingChangeEventSourceMetrics implements MongoDbStreamingChangeEventSourceMetricsMBean {

    private AtomicLong numberOfPrimaryElections = new AtomicLong();
    private AtomicLong numberOfDisconnects = new AtomicLong();

    <T extends CdcSourceTaskContext> MongoDbStreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                              EventMetadataProvider eventMetadataProvider) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public long getNumberOfPrimaryElections() {
        return numberOfPrimaryElections.get();
    }

    @Override
    public long getNumberOfDisconnects() {
        return numberOfDisconnects.get();
    }

    @Override
    public void onMetadataEvent(MetadataEvent event) {
        if (event instanceof PrimaryElectionEvent) {
            numberOfPrimaryElections.incrementAndGet();
        }
        else if (event instanceof DisconnectEvent) {
            numberOfDisconnects.incrementAndGet();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.numberOfPrimaryElections.set(0);
        this.numberOfDisconnects.set(0);
    }
}
