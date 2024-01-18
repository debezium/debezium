/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Oracle Streaming Metrics implementation for the Oracle OpenLogReplicator adapter.
 *
 * @author Chris Cranford
 */
public class OpenLogReplicatorStreamingChangeEventSourceMetrics
        extends AbstractOracleStreamingChangeEventSourceMetrics
        implements OpenLogReplicatorStreamingChangeEventSourceMetricsMXBean {

    private final AtomicReference<Scn> checkpointScn = new AtomicReference<>(Scn.NULL);
    private final AtomicLong checkpointIndex = new AtomicLong();
    private final AtomicLong processedEventsCount = new AtomicLong();

    public OpenLogReplicatorStreamingChangeEventSourceMetrics(CdcSourceTaskContext taskContext,
                                                              ChangeEventQueueMetrics changeEventQueueMetrics,
                                                              EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public BigInteger getCheckpointScn() {
        return checkpointScn.get().asBigInteger();
    }

    @Override
    public long getCheckpointIndex() {
        return checkpointIndex.get();
    }

    @Override
    public long getProcessedEventCount() {
        return processedEventsCount.get();
    }

    public void setCheckpointDetails(Scn checkpointScn, Long checkpointIndex) {
        this.checkpointScn.set(checkpointScn);
        this.checkpointIndex.set(checkpointIndex);
    }

    public void incrementProcessedEventsCount() {
        processedEventsCount.incrementAndGet();
    }
}
