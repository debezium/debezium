/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class StreamingChangeEventSourceMetrics extends Metrics implements StreamingChangeEventSourceMetricsMXBean, DataChangeEventListener {

    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicLong secondsBehindSource = new AtomicLong(-1);
    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<Map<String, String>>(Collections.emptyMap());
    private volatile String lastTransactionId;

    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics) {
        super(taskContext, "streaming", changeEventQueueMetrics);
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    @Override
    public String[] getMonitoredTables() {
        return taskContext.capturedDataCollections();
    }

    public void connected(boolean connected) {
        this.connected.set(connected);
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return sourceEventPosition.get();
    }

    @Override
    public long getSecondsBehindSource() {
        return secondsBehindSource.get();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return numberOfCommittedTransactions.get();
    }

    @Override
    public void onEvent(Object source, OffsetContext offset, Object key, Struct value, EventMetadataProvider metadataProvider) {
        super.onEvent(source, offset, key, value, metadataProvider);

        final long eventTimestamp = metadataProvider.getEventTimestamp(source, offset, key, value);
        if (eventTimestamp != -1) {
            secondsBehindSource.set((System.currentTimeMillis() - eventTimestamp) / 1000);
        }

        final String transactionId = metadataProvider.getTransactionId(source, offset, key, value);
        if (transactionId != null) {
            if (!transactionId.equals(lastTransactionId)) {
                lastTransactionId = transactionId;
                numberOfCommittedTransactions.incrementAndGet();
            }
        }

        final Map<String, String> eventSource = metadataProvider.getEventSourcePosition(source, offset, key, value);
        if (eventSource != null) {
            sourceEventPosition.set(eventSource);
        }
    }

    @Override
    public void reset() {
        super.reset();
        connected.set(false);
        secondsBehindSource.set(-1);
        numberOfCommittedTransactions.set(0);
        sourceEventPosition.set(Collections.emptyMap());
        lastTransactionId = null;
    }
}
