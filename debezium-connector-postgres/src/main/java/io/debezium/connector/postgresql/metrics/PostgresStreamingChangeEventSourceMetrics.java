/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

@ThreadSafe
public class PostgresStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<PostgresPartition>
        implements PostgresStreamingChangeEventSourceMetricsMXBean {

    private final AtomicLong totalNumberOfLogicalMessageEventsSeen = new AtomicLong();

    public <T extends CdcSourceTaskContext> PostgresStreamingChangeEventSourceMetrics(T taskContext,
                                                                                      ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                      EventMetadataProvider eventMetadataProvider,
                                                                                      CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider, capturedTablesSupplier);
    }

    @Override
    public void onEvent(PostgresPartition partition, DataCollectionId source, OffsetContext offset, Object key, Struct value, Operation operation) {
        super.onEvent(partition, source, offset, key, value, operation);
        if (operation == Operation.MESSAGE) {
            totalNumberOfLogicalMessageEventsSeen.incrementAndGet();
        }
    }

    @Override
    public void onFilteredEvent(PostgresPartition partition, String event, Operation operation) {
        super.onFilteredEvent(partition, event, operation);
        if (operation == Operation.MESSAGE) {
            totalNumberOfLogicalMessageEventsSeen.incrementAndGet();
        }
    }

    @Override
    public long getTotalNumberOfLogicalMessageEventsSeen() {
        return totalNumberOfLogicalMessageEventsSeen.get();
    }

    @Override
    public void reset() {
        super.reset();
        totalNumberOfLogicalMessageEventsSeen.set(0);
    }
}
