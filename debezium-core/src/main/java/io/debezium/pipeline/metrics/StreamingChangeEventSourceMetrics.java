/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;

/**
 * @author Randall Hauch, Jiri Pechanec
 */
public class StreamingChangeEventSourceMetrics extends Metrics implements StreamingChangeEventSourceMetricsMXBean, DataChangeEventListener {

    private AtomicBoolean connected = new AtomicBoolean();

    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics(T taskContext) {
        super(taskContext, "streaming");
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    @Override
    public String[] getMonitoredTables() {
        return new String[] {};
    }
}
