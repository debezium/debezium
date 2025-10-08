/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class MongoDbChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<MongoDbPartition> {

    private MongoDbSnapshotChangeEventSourceMetrics snapshotMetrics;
    private MongoDbStreamingChangeEventSourceMetrics streamingMetrics;

    @Override
    public <T extends CdcSourceTaskContext> MongoDbSnapshotChangeEventSourceMetrics getSnapshotMetrics(
                                                                                                       T taskContext,
                                                                                                       ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                       EventMetadataProvider eventMetadataProvider) {
        if (snapshotMetrics == null) {
            snapshotMetrics = new MongoDbSnapshotChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);

        }
        return snapshotMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> MongoDbStreamingChangeEventSourceMetrics getStreamingMetrics(
                                                                                                         T taskContext,
                                                                                                         ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                         EventMetadataProvider eventMetadataProvider,
                                                                                                         CapturedTablesSupplier capturedTablesSupplier) {
        if (streamingMetrics == null) {
            streamingMetrics = new MongoDbStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider, capturedTablesSupplier);
        }
        return streamingMetrics;
    }
}
