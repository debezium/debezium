/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.metrics;

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

    private final AtomicLong totalNumberOfWrites = new AtomicLong();
    private final AtomicLong totalNumberOfDeletes = new AtomicLong();
    private final AtomicLong totalNumberOfTruncates = new AtomicLong();
    private final AtomicLong totalNumberOfFilteredEvents = new AtomicLong();
    private final AtomicLong totalNumberOfTablesCreated = new AtomicLong();
    private final AtomicLong totalNumberOfTablesAltered = new AtomicLong();

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
    public void written(long count) {
        totalNumberOfWrites.addAndGet(count);
    }

    @Override
    public void deleted(long count) {
        totalNumberOfDeletes.addAndGet(count);
    }

    @Override
    public void truncated() {
        totalNumberOfTruncates.incrementAndGet();
    }

    @Override
    public void filtered() {
        totalNumberOfFilteredEvents.incrementAndGet();
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
    public long getTotalNumberOfWrites() {
        return totalNumberOfWrites.get();
    }

    @Override
    public long getTotalNumberOfDeletes() {
        return totalNumberOfDeletes.get();
    }

    @Override
    public long getTotalNumberOfTruncates() {
        return totalNumberOfTruncates.get();
    }

    @Override
    public long getTotalNumberOfFilteredEvents() {
        return totalNumberOfFilteredEvents.get();
    }

    @Override
    public long getTotalNumberOfTablesCreated() {
        return totalNumberOfTablesCreated.get();
    }

    @Override
    public long getTotalNumberOfTablesAltered() {
        return totalNumberOfTablesAltered.get();
    }

}
