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
import io.debezium.metrics.event.LagBehindSourceEvent;
import io.debezium.metrics.stats.LagBehindSourceMeasurement;
import io.debezium.metrics.stats.LongDDSketchStatistics;
import io.debezium.metrics.stats.MeasurementCollector;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.traits.StreamingMetricsMXBean;
import io.debezium.pipeline.metrics.traits.StreamingStatisticsMXBean;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Carries streaming metrics.
 */
@ThreadSafe
public class StreamingMeter implements StreamingMetricsMXBean, StreamingStatisticsMXBean {

    private final AtomicLong numberOfCommittedTransactions = new AtomicLong();
    private final AtomicReference<Map<String, String>> sourceEventPosition = new AtomicReference<>(Collections.emptyMap());
    private final AtomicReference<String> lastTransactionId = new AtomicReference<>();
    private final MeasurementCollector<LagBehindSourceEvent> measurementCollector = new MeasurementCollector<>();
    private final LagBehindSourceMeasurement lagBehindSourceMeasurement = new LagBehindSourceMeasurement(new LongDDSketchStatistics<>());

    private final CapturedTablesSupplier capturedTablesSupplier;
    private final EventMetadataProvider metadataProvider;

    public StreamingMeter(CapturedTablesSupplier capturedTablesSupplier, EventMetadataProvider metadataProvider) {
        this.capturedTablesSupplier = capturedTablesSupplier != null ? capturedTablesSupplier : Collections::emptyList;
        this.metadataProvider = metadataProvider;
        this.measurementCollector.addMeasurement(LagBehindSourceEvent.class, lagBehindSourceMeasurement);
    }

    @Override
    public String[] getCapturedTables() {
        return capturedTablesSupplier.getCapturedTables()
                .stream()
                .map(DataCollectionId::toString)
                .toArray(String[]::new);
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return sourceEventPosition.get();
    }

    @Override
    public long getMilliSecondsBehindSource() {
        final Long lag = lagBehindSourceMeasurement.getLastValue();
        return lag != null ? lag : -1;
    }

    @Override
    public Long getMilliSecondsBehindSourceMinValue() {
        return lagBehindSourceMeasurement.getMinValue();
    }

    @Override
    public Long getMilliSecondsBehindSourceMaxValue() {
        return lagBehindSourceMeasurement.getMaxValue();
    }

    @Override
    public Long getMilliSecondsBehindSourceAverageValue() {
        return lagBehindSourceMeasurement.getAverageValue();
    }

    @Override
    public Double getMilliSecondsBehindSourceP50() {
        return lagBehindSourceMeasurement.getValueAtP50();
    }

    @Override
    public Double getMilliSecondsBehindSourceP95() {
        return lagBehindSourceMeasurement.getValueAtP95();
    }

    @Override
    public Double getMilliSecondsBehindSourceP99() {
        return lagBehindSourceMeasurement.getValueAtP99();
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
    public void resetLagBehindSource() {
        lagBehindSourceMeasurement.reset();
    }

    public void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        final Instant eventTimestamp = metadataProvider.getEventTimestamp(source, offset, key, value);
        if (eventTimestamp != null) {
            final Duration lag = Duration.between(eventTimestamp, Instant.now());
            measurementCollector.accept(new LagBehindSourceEvent(lag.toMillis()));
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
        lagBehindSourceMeasurement.reset();
        numberOfCommittedTransactions.set(0);
        sourceEventPosition.set(Collections.emptyMap());
        lastTransactionId.set(null);
    }
}
