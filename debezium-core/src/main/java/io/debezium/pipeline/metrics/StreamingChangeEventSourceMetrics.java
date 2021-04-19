/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class StreamingChangeEventSourceMetrics<P extends TaskPartition>
        extends PipelineMetrics<P, StreamingChangeEventSourcePartitionMetrics>
        implements StreamingChangeEventSourceTaskMetricsMXBean, DataChangeEventListener<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingChangeEventSourceMetrics.class);

    private final AtomicBoolean connected = new AtomicBoolean();

    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                              EventMetadataProvider metadataProvider,
                                                                              Collection<P> partitions) {
        super(taskContext, "streaming", changeEventQueueMetrics, partitions,
                (P partition) -> new StreamingChangeEventSourcePartitionMetrics(taskContext, "streaming",
                        partition, metadataProvider));
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    public void connected(boolean connected) {
        this.connected.set(connected);
        LOGGER.info("Connected metrics set to '{}'", this.connected.get());
    }

    @Override
    public void onConnectorEvent(TaskPartition partition, ConnectorEvent event) {
    }

    @Override
    public void reset() {
        super.reset();
        connected.set(false);
    }
}
