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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * Metrics specific to streaming change event sources scoped to a source partition
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class StreamingChangeEventSourcePartitionMetrics extends ChangeEventSourcePartitionMetrics
        implements StreamingChangeEventSourcePartitionMetricsMXBean {

    private final AtomicReference<Duration> lagBehindSource = new AtomicReference<>();
    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<>(Collections.emptyMap());
    private final AtomicReference<String> lastTransactionId = new AtomicReference<>();

    public <T extends CdcSourceTaskContext> StreamingChangeEventSourcePartitionMetrics(T taskContext, String contextName,
                                                                                       TaskPartition partition,
                                                                                       EventMetadataProvider metadataProvider) {
        super(taskContext, contextName, partition, metadataProvider);
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

    @Override
    public String[] getMonitoredTables() {
        // TODO: return only current partition tables
        return taskContext.capturedDataCollections();
    }

    @Override
    public void reset() {
        lagBehindSource.set(null);
        numberOfCommittedTransactions.set(0);
        sourceEventPosition.set(Collections.emptyMap());
        lastTransactionId.set(null);
    }
}
