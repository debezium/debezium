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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.JmxUtils;
import io.debezium.sink.spi.SinkProgressListener;

/**
 * JMX metrics for the JDBC sink connector. Counters are updated on the record-processing path and
 * exposed under the {@code debezium.jdbc:type=connector-metrics,context=sink,...} object name,
 * consistent with how Debezium source connectors expose their metrics.
 *
 */
@ThreadSafe
public class JdbcSinkConnectorMetrics implements JdbcSinkConnectorMetricsMXBean, SinkProgressListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorMetrics.class);

    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.jdbc:type=connector-metrics,context=sink,server=%s,task=%s";

    private final ObjectName objectName;

    private final AtomicLong totalNumberOfInsertEventsSeen = new AtomicLong();
    private final AtomicLong totalNumberOfUpdateEventsSeen = new AtomicLong();
    private final AtomicLong totalNumberOfUpsertEventsSeen = new AtomicLong();
    private final AtomicLong totalNumberOfDeleteEventsSeen = new AtomicLong();
    private final AtomicLong totalNumberOfTruncateEventsSeen = new AtomicLong();
    private final AtomicLong numberOfFilteredEvents = new AtomicLong();
    private final AtomicLong totalNumberOfTablesCreated = new AtomicLong();
    private final AtomicLong totalNumberOfTablesAltered = new AtomicLong();
    private final AtomicBoolean connected = new AtomicBoolean();

    public JdbcSinkConnectorMetrics(String connectorName, String taskId) {
        final String name = String.format(JMX_OBJECT_NAME_FORMAT, Sanitizer.jmxSanitize(connectorName), Sanitizer.jmxSanitize(taskId));
        try {
            this.objectName = new ObjectName(name);
        }
        catch (MalformedObjectNameException e) {
            throw new DebeziumException("Invalid metric name '" + name + "'", e);
        }
    }

    public void register() {
        JmxUtils.registerMXBean(objectName, this);
    }

    public void unregister() {
        JmxUtils.unregisterMXBean(objectName);
    }

    @Override
    public void inserted() {
        totalNumberOfInsertEventsSeen.incrementAndGet();
    }

    @Override
    public void updated() {
        totalNumberOfUpdateEventsSeen.incrementAndGet();
    }

    @Override
    public void upserted() {
        totalNumberOfUpsertEventsSeen.incrementAndGet();
    }

    @Override
    public void deleted() {
        totalNumberOfDeleteEventsSeen.incrementAndGet();
    }

    @Override
    public void truncated() {
        totalNumberOfTruncateEventsSeen.incrementAndGet();
    }

    @Override
    public void filtered() {
        numberOfFilteredEvents.incrementAndGet();
    }

    @Override
    public void tableCreated() {
        totalNumberOfTablesCreated.incrementAndGet();
    }

    @Override
    public void tableAltered() {
        totalNumberOfTablesAltered.incrementAndGet();
    }

    @Override
    public void connected(boolean connected) {
        this.connected.set(connected);
    }

    @Override
    public long getTotalNumberOfInsertEventsSeen() {
        return totalNumberOfInsertEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfUpdateEventsSeen() {
        return totalNumberOfUpdateEventsSeen.get();
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
    public long getTotalNumberOfTablesCreated() {
        return totalNumberOfTablesCreated.get();
    }

    @Override
    public long getTotalNumberOfTablesAltered() {
        return totalNumberOfTablesAltered.get();
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }
}
