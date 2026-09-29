/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class PostgresChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<PostgresPartition> {

    @Override
    public <T extends CdcSourceTaskContext> PostgresStreamingChangeEventSourceMetrics getStreamingMetrics(
                                                                                                          T taskContext,
                                                                                                          ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                          EventMetadataProvider eventMetadataProvider,
                                                                                                          CapturedTablesSupplier capturedTablesSupplier) {
        return new PostgresStreamingChangeEventSourceMetrics(
                taskContext, changeEventQueueMetrics, eventMetadataProvider, capturedTablesSupplier);
    }
}
