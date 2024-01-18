/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.time.Clock;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleStreamingMetricsTest;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Oracle XStream Streaming Metrics Tests
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.XSTREAM)
public class XStreamStreamingMetricsTest extends OracleStreamingMetricsTest<XStreamStreamingChangeEventSourceMetrics> {

    @Override
    protected XStreamStreamingChangeEventSourceMetrics createMetrics(OracleTaskContext taskContext,
                                                                     ChangeEventQueue<DataChangeEvent> queue,
                                                                     EventMetadataProvider metadataProvider,
                                                                     OracleConnectorConfig connectorConfig,
                                                                     Clock clock) {
        return new XStreamStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider);
    }

}
