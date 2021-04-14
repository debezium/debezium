/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * Base for metrics implementations.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public abstract class PipelineMetrics<P extends TaskPartition, B extends ChangeEventSourcePartitionMetrics>
        extends Metrics implements DataChangeEventListener<P>, ChangeEventSourceTaskMetricsMXBean {

    private final ChangeEventQueueMetrics changeEventQueueMetrics;
    protected final CdcSourceTaskContext taskContext;
    private final Map<P, B> beans = new HashMap<>();

    protected <T extends CdcSourceTaskContext> PipelineMetrics(T taskContext, String contextName, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                               Collection<P> partitions,
                                                               Function<P, B> beanFactory) {
        super(taskContext, contextName);
        this.taskContext = taskContext;
        this.changeEventQueueMetrics = changeEventQueueMetrics;

        for (P partition : partitions) {
            beans.put(partition, beanFactory.apply(partition));
        }
    }

    @Override
    public synchronized void register(Logger logger) {
        super.register(logger);

        beans.values().forEach(bean -> bean.register(logger));
    }

    @Override
    public synchronized void unregister(Logger logger) {
        beans.values().forEach(bean -> bean.unregister(logger));

        super.unregister(logger);
    }

    @Override
    public void onEvent(P partition, DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        onPartitionEvent(partition, bean -> bean.onEvent(source, offset, key, value));
    }

    @Override
    public void onFilteredEvent(P partition, String event) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event));
    }

    @Override
    public void onErroneousEvent(P partition, String event) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event));
    }

    @Override
    public void onConnectorEvent(P partition, ConnectorEvent event) {
    }

    protected void onPartitionEvent(P partition, Consumer<B> handler) {
        B bean = beans.get(partition);
        if (bean == null) {
            throw new IllegalArgumentException("Metrics bean for partition " + partition + "is not registered");
        }
        handler.accept(bean);
    }

    @Override
    public void reset() {
        beans.values().forEach(B::reset);
    }

    @Override
    public int getQueueTotalCapacity() {
        return changeEventQueueMetrics.totalCapacity();
    }

    @Override
    public int getQueueRemainingCapacity() {
        return changeEventQueueMetrics.remainingCapacity();
    }

    @Override
    public long getMaxQueueSizeInBytes() {
        return changeEventQueueMetrics.maxQueueSizeInBytes();
    }

    @Override
    public long getCurrentQueueSizeInBytes() {
        return changeEventQueueMetrics.currentQueueSizeInBytes();
    }

}
