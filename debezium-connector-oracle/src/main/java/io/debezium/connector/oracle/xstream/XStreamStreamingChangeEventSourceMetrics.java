/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Oracle Streaming Metrics implementation for the Oracle XStream streaming adapter.
 *
 * @author Chris Cranford
 */
public class XStreamStreamingChangeEventSourceMetrics extends AbstractOracleStreamingChangeEventSourceMetrics {
    public XStreamStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext,
                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                    EventMetadataProvider metadataProvider,
                                                    CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }
}
