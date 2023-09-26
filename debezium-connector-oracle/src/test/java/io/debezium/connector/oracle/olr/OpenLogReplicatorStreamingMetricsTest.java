/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;

import org.junit.Test;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleStreamingMetricsTest;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * OpenLogReplicator Streaming Metrics Tests
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.OLR)
public class OpenLogReplicatorStreamingMetricsTest extends OracleStreamingMetricsTest<OpenLogReplicatorStreamingChangeEventSourceMetrics> {

    @Override
    protected OpenLogReplicatorStreamingChangeEventSourceMetrics createMetrics(OracleTaskContext taskContext,
                                                                               ChangeEventQueue<DataChangeEvent> queue,
                                                                               EventMetadataProvider metadataProvider,
                                                                               OracleConnectorConfig connectorConfig,
                                                                               Clock clock) {
        return new OpenLogReplicatorStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider);
    }

    @Test
    public void testCheckpointDetalisMetrics() {
        metrics.setCheckpointDetails(Scn.valueOf("12345"), 98765L);
        assertThat(metrics.getCheckpointScn()).isEqualTo(Scn.valueOf("12345").asBigInteger());
        assertThat(metrics.getCheckpointIndex()).isEqualTo(98765L);
    }

}
