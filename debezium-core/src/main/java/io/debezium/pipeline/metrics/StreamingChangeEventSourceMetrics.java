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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class StreamingChangeEventSourceMetrics extends PipelineMetrics implements StreamingChangeEventSourceMetricsMXBean, DataChangeEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingChangeEventSourceMetrics.class);

    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicReference<Duration> lagBehindSource = new AtomicReference<>();
    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<Map<String, String>>(Collections.emptyMap());
    private final AtomicReference<String> lastTransactionId = new AtomicReference<>();

    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                              EventMetadataProvider metadataProvider) {
        super(taskContext, "streaming", changeEventQueueMetrics, metadataProvider);
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
        LOGGER.info("Connected metrics set to '{}'", this.connected.get());
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
    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        super.onEvent(source, offset, key, value);

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
