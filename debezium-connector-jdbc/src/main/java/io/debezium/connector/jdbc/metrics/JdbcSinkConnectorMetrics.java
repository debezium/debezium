/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.JmxUtils;

/**
 * JMX metrics for the JDBC sink connector. Counters are updated on the record-processing path and
 * exposed under the {@code debezium.jdbc:type=connector-metrics,context=sink,...} object name,
 * consistent with how Debezium source connectors expose their metrics.
 *
 * @author Lee Jaemin
 */
@ThreadSafe
public class JdbcSinkConnectorMetrics implements JdbcSinkConnectorMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorMetrics.class);

    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.jdbc:type=connector-metrics,context=sink,server=%s,task=%s";

    private final ObjectName objectName;

    private final AtomicLong totalNumberOfUpsertEventsSeen = new AtomicLong();
    private final AtomicLong totalNumberOfDeleteEventsSeen = new AtomicLong();
    private final AtomicLong totalNumberOfTruncateEventsSeen = new AtomicLong();
    private final AtomicLong numberOfFilteredEvents = new AtomicLong();
    private final AtomicBoolean connected = new AtomicBoolean();

    public JdbcSinkConnectorMetrics(String connectorName, String taskId) {
        final String name = String.format(JMX_OBJECT_NAME_FORMAT, Sanitizer.jmxSanitize(connectorName), Sanitizer.jmxSanitize(taskId));
        try {
            this.objectName = new ObjectName(name);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + name + "'", e);
        }
    }

    public void register() {
        JmxUtils.registerMXBean(objectName, this);
    }

    public void unregister() {
        JmxUtils.unregisterMXBean(objectName);
    }

    public void onUpsert() {
        totalNumberOfUpsertEventsSeen.incrementAndGet();
    }

    public void onDelete() {
        totalNumberOfDeleteEventsSeen.incrementAndGet();
    }

    public void onTruncate() {
        totalNumberOfTruncateEventsSeen.incrementAndGet();
    }

    public void onFilteredEvent() {
        numberOfFilteredEvents.incrementAndGet();
    }

    public void connected(boolean connected) {
        this.connected.set(connected);
    }

    @Override
    public long getTotalNumberOfUpsertEventsSeen() {
        return totalNumberOfUpsertEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfDeleteEventsSeen() {
        return totalNumberOfDeleteEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfTruncateEventsSeen() {
        return totalNumberOfTruncateEventsSeen.get();
    }

    @Override
    public long getNumberOfFilteredEvents() {
        return numberOfFilteredEvents.get();
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }
}
