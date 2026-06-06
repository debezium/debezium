/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mariadb.MariaDbPartition;
import io.debezium.connector.mariadb.MariaDbTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Implementation of the {@link io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<MariaDbPartition> {

    public MariaDbStreamingChangeEventSourceMetrics streamingMetrics;

    public MariaDbChangeEventSourceMetricsFactory(MariaDbStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<MariaDbPartition> getSnapshotMetrics(
                                                                                                                  T taskContext,
                                                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                  EventMetadataProvider eventMetadataProvider) {
        return new MariaDbSnapshotChangeEventSourceMetrics((MariaDbTaskContext) taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<MariaDbPartition> getStreamingMetrics(
                                                                                                                    T taskContext,
                                                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                    EventMetadataProvider eventMetadataProvider,
                                                                                                                    CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }

    @Override
    public boolean connectionMetricHandledByCoordinator() {
        return false;
    }
}
