/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.ChangeEventSourceMetrics;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * Base implementation of task-scoped multi-partition SQL Server connector metrics.
 */
abstract class AbstractSqlServerTaskMetrics<B extends AbstractSqlServerPartitionMetrics> extends Metrics
        implements ChangeEventSourceMetrics<SqlServerPartition>, SqlServerTaskMetricsMXBean {

    private final ChangeEventQueueMetrics changeEventQueueMetrics;
    private final Map<SqlServerPartition, B> beans = new HashMap<>();

    AbstractSqlServerTaskMetrics(CdcSourceTaskContext taskContext,
                                 String contextName,
                                 ChangeEventQueueMetrics changeEventQueueMetrics,
                                 Collection<SqlServerPartition> partitions,
                                 Function<SqlServerPartition, B> beanFactory) {
        super(taskContext, Collect.linkMapOf(
                "server", taskContext.getConnectorName(),
                "task", taskContext.getTaskId(),
                "context", contextName));
        this.changeEventQueueMetrics = changeEventQueueMetrics;

        for (SqlServerPartition partition : partitions) {
            beans.put(partition, beanFactory.apply(partition));
        }
    }

    @Override
    public synchronized void register() {
        super.register();
        beans.values().forEach(Metrics::register);
    }

    @Override
    public synchronized void unregister() {
        beans.values().forEach(Metrics::unregister);
        super.unregister();
    }

    @Override
    public void reset() {
        beans.values().forEach(B::reset);
    }

    @Override
    public void onEvent(SqlServerPartition partition, DataCollectionId source, OffsetContext offset, Object key,
                        Struct value, Operation operation) {
        onPartitionEvent(partition, bean -> bean.onEvent(source, offset, key, value, operation));
    }

    @Override
    public void onFilteredEvent(SqlServerPartition partition, String event) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event));
    }

    @Override
    public void onFilteredEvent(SqlServerPartition partition, String event, Operation operation) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event, operation));
    }

    @Override
    public void onErroneousEvent(SqlServerPartition partition, String event) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event));
    }

    @Override
    public void onErroneousEvent(SqlServerPartition partition, String event, Operation operation) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event, operation));
    }

    @Override
    public void onConnectorEvent(SqlServerPartition partition, ConnectorEvent event) {
        onPartitionEvent(partition, bean -> bean.onConnectorEvent(event));
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

    protected void onPartitionEvent(SqlServerPartition partition, Consumer<B> handler) {
        B bean = beans.get(partition);
        if (bean == null) {
            throw new IllegalArgumentException("MBean for partition " + partition + " are not registered");
        }
        handler.accept(bean);
    }
}
