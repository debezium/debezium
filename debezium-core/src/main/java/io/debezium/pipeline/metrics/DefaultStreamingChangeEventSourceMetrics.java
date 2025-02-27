/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope.Operation;
import io.debezium.metrics.activity.ActivityMonitoringMeter;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.meters.ConnectionMeter;
import io.debezium.pipeline.meters.StreamingMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The default implementation of metrics related to the streaming phase of a connector.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class DefaultStreamingChangeEventSourceMetrics<P extends Partition> extends PipelineMetrics<P>
        implements StreamingChangeEventSourceMetrics<P>, StreamingChangeEventSourceMetricsMXBean {

    private final ConnectionMeter connectionMeter;
    private final StreamingMeter streamingMeter;
    private final ActivityMonitoringMeter activityMonitoringMeter;

    public <T extends CdcSourceTaskContext> DefaultStreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                     EventMetadataProvider metadataProvider) {
        super(taskContext, "streaming", changeEventQueueMetrics, metadataProvider);
        streamingMeter = new StreamingMeter(taskContext, metadataProvider);
        connectionMeter = new ConnectionMeter();
        activityMonitoringMeter = new ActivityMonitoringMeter();
    }

    public <T extends CdcSourceTaskContext> DefaultStreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                     EventMetadataProvider metadataProvider, Map<String, String> tags) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, tags);
        streamingMeter = new StreamingMeter(taskContext, metadataProvider);
        connectionMeter = new ConnectionMeter();
        activityMonitoringMeter = new ActivityMonitoringMeter();
    }

    @Override
    public boolean isConnected() {
        return connectionMeter.isConnected();
    }

    @Override
    public String[] getCapturedTables() {
        return streamingMeter.getCapturedTables();
    }

    public void connected(boolean connected) {
        connectionMeter.connected(connected);
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return streamingMeter.getSourceEventPosition();
    }

    @Override
    public long getMilliSecondsBehindSource() {
        return streamingMeter.getMilliSecondsBehindSource();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return streamingMeter.getNumberOfCommittedTransactions();
    }

    @Override
    public void onEvent(P partition, DataCollectionId source, OffsetContext offset, Object key, Struct value, Operation operation) {
        super.onEvent(partition, source, offset, key, value, operation);
        streamingMeter.onEvent(source, offset, key, value);
        if (taskContext.getConfig().isAdvancedMetricsEnabled()) {
            activityMonitoringMeter.onEvent(source, offset, key, value, operation);
        }
    }

    @Override
    public void onConnectorEvent(P partition, ConnectorEvent event) {
    }

    @Override
    public String getLastTransactionId() {
        return streamingMeter.getLastTransactionId();
    }

    @Override
    public void reset() {
        super.reset();
        streamingMeter.reset();
        connectionMeter.reset();
        activityMonitoringMeter.reset();
    }

    @Override
    public void pause() {
        activityMonitoringMeter.pause();
    }

    @Override
    public void resume() {
        activityMonitoringMeter.resume();
    }

    @Override
    public Map<String, Long> getNumberOfCreateEventsSeen() {
        return activityMonitoringMeter.getNumberOfCreateEventsSeen();
    }

    @Override
    public Map<String, Long> getNumberOfDeleteEventsSeen() {
        return activityMonitoringMeter.getNumberOfDeleteEventsSeen();
    }

    @Override
    public Map<String, Long> getNumberOfUpdateEventsSeen() {
        return activityMonitoringMeter.getNumberOfUpdateEventsSeen();
    }

    @Override
    public Map<String, Long> getNumberOfTruncateEventsSeen() {
        return activityMonitoringMeter.getNumberOfTruncateEventsSeen();
    }
}
