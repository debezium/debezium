/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * The default implementation of metrics related to the streaming phase of a connector.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class DefaultStreamingChangeEventSourceMetrics extends PipelineMetrics
        implements StreamingChangeEventSourceMetrics, StreamingChangeEventSourceMetricsMXBean {

    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicReference<Duration> lagBehindSource = new AtomicReference<>();
    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<Map<String, String>>(Collections.emptyMap());
    private final AtomicReference<String> lastTransactionId = new AtomicReference<>();

    public <T extends CdcSourceTaskContext> DefaultStreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                     EventMetadataProvider metadataProvider) {
        super(taskContext, "streaming", changeEventQueueMetrics, metadataProvider);
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    /**
     * @deprecated Superseded by the 'Captured Tables' metric. Use {@link #getCapturedTables()}.
     * Scheduled for removal in a future release.
     */
    @Override
    @Deprecated
    public String[] getMonitoredTables() {
        return taskContext.capturedDataCollections();
    }

    @Override
    public String[] getCapturedTables() {
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
    public long getMilliSecondsBehindSource() {
        Duration lag = lagBehindSource.get();
        return lag != null ? lag.toMillis() : -1;
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return numberOfCommittedTransactions.get();
    }

    @Override
    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Operation operation) {
        super.onEvent(source, offset, key, value, operation);

        final Instant eventTimestamp = metadataProvider.getEventTimestamp(source, offset, key, value);
        if (eventTimestamp != null) {
            lagBehindSource.set(Duration.between(eventTimestamp, Instant.now()));
        }

        final String transactionId = metadataProvider.getTransactionId(source, offset, key, value);
        if (transactionId != null) {
            if (!transactionId.equals(lastTransactionId.get())) {
                lastTransactionId.set(transactionId);
                numberOfCommittedTransactions.incrementAndGet();
            }
        }

        final Map<String, String> eventSource = metadataProvider.getEventSourcePosition(source, offset, key, value);
        if (eventSource != null) {
            sourceEventPosition.set(eventSource);
        }
    }

    @Override
    public void onConnectorEvent(ConnectorEvent event) {
    }

    @Override
    public String getLastTransactionId() {
        return lastTransactionId.get();
    }

    @Override
    public void reset() {
        super.reset();
        connected.set(false);
        lagBehindSource.set(null);
        numberOfCommittedTransactions.set(0);
        sourceEventPosition.set(Collections.emptyMap());
        lastTransactionId.set(null);
    }
}
