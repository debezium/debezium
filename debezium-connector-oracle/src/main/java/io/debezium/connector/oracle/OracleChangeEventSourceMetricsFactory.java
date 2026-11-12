/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class OracleChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<OraclePartition> {

    private final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final boolean connectionMetricHandledByCoordinator;

    public OracleChangeEventSourceMetricsFactory(AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics,
                                                 OracleConnectorConfig connectorConfig) {
        this.streamingMetrics = streamingMetrics;
        this.connectionMetricHandledByCoordinator = connectorConfig.getAdapter().isConnectionMetricHandledByCoordinator();
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<OraclePartition> getStreamingMetrics(T taskContext,
                                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                   EventMetadataProvider eventMetadataProvider,
                                                                                                                   CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }

    @Override
    public boolean connectionMetricHandledByCoordinator() {
        return connectionMetricHandledByCoordinator;
    }
}
