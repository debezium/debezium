/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.traits.StreamingMetricsMXBean;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Carries streaming metrics.
 */
@ThreadSafe
public class StreamingMeter implements StreamingMetricsMXBean {

    private final AtomicReference<Duration> lagBehindSource = new AtomicReference<>();
    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<>(Collections.emptyMap());
    private final AtomicReference<String> lastTransactionId = new AtomicReference<>();

    private final CdcSourceTaskContext taskContext;
    private final EventMetadataProvider metadataProvider;

    public StreamingMeter(CdcSourceTaskContext taskContext, EventMetadataProvider metadataProvider) {
        this.taskContext = taskContext;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public String[] getCapturedTables() {
        return taskContext.capturedDataCollections();
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
    public String getLastTransactionId() {
        return lastTransactionId.get();
    }

    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
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

    public void reset() {
        lagBehindSource.set(null);
        numberOfCommittedTransactions.set(0);
        sourceEventPosition.set(Collections.emptyMap());
        lastTransactionId.set(null);
    }
}
