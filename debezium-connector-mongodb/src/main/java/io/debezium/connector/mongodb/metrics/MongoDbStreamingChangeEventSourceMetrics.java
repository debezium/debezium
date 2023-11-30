/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BsonDocument;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.DisconnectEvent;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.connector.mongodb.PrimaryElectionEvent;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Clock;
import io.debezium.util.Collect;

/**
 * @author Chris Cranford
 */
@ThreadSafe
public class MongoDbStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<MongoDbPartition>
        implements MongoDbStreamingChangeEventSourceMetricsMBean {

    private final AtomicLong numberOfPrimaryElections = new AtomicLong();
    private final AtomicLong numberOfDisconnects = new AtomicLong();
    private final AtomicLong lastSourceEventPollTime = new AtomicLong();
    private final AtomicLong lastEmptyPollTime = new AtomicLong();
    private final AtomicLong numberOfSourceEvents = new AtomicLong();
    private final AtomicLong numberOfEmptyPolls = new AtomicLong();

    public <T extends CdcSourceTaskContext> MongoDbStreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                     EventMetadataProvider eventMetadataProvider) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider, Collect.linkMapOf(
                "context", "streaming",
                "server", taskContext.getConnectorName(),
                "task", taskContext.getTaskId()));
    }

    @Override
    public long getNumberOfPrimaryElections() {
        return numberOfPrimaryElections.get();
    }

    @Override
    public long getNumberOfDisconnects() {
        return numberOfDisconnects.get();
    }

    @Override
    public long getLastSourceEventPollTime() {
        return lastSourceEventPollTime.get();
    }

    @Override
    public long getLastEmptyPollTime() {
        return lastEmptyPollTime.get();
    }

    @Override
    public long getNumberOfEmptyPolls() {
        return numberOfEmptyPolls.get();
    }

    public void onSourceEventPolled(ChangeStreamDocument<BsonDocument> event, Clock clock, Instant prePollTimestamp) {
        var now = clock.currentTimeAsInstant();
        var duration = Duration.between(prePollTimestamp, now).toMillis();

        if (event == null) {
            lastEmptyPollTime.set(duration);
            numberOfEmptyPolls.incrementAndGet();
        }
        else {
            lastSourceEventPollTime.set(duration);
        }
    }

    @Override
    public void onConnectorEvent(MongoDbPartition partition, ConnectorEvent event) {
        if (event instanceof PrimaryElectionEvent) {
            numberOfPrimaryElections.incrementAndGet();
        }
        else if (event instanceof DisconnectEvent) {
            numberOfDisconnects.incrementAndGet();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.numberOfPrimaryElections.set(0);
        this.numberOfDisconnects.set(0);
    }
}
