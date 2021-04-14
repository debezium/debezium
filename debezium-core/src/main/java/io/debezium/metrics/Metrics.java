/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.TaskPartition;

/**
 * Base for metrics implementations.
 *
 * @author Jiri Pechanec
 */
@ThreadSafe
public abstract class Metrics {

    private final ObjectName name;
    private volatile boolean registered = false;

    protected Metrics(CdcSourceTaskContext taskContext, String contextName) {
        this.name = metricName(taskContext.getConnectorType(), taskContext.getConnectorName(), contextName);
    }

    protected Metrics(CdcSourceTaskContext taskContext, String contextName, TaskPartition partition) {
        this.name = metricName(taskContext.getConnectorType(), taskContext.getConnectorName(), contextName,
                partition.getSourcePartition());
    }

    protected Metrics(CommonConnectorConfig connectorConfig, String contextName) {
        this.name = metricName(connectorConfig.getContextName(), connectorConfig.getLogicalName(), contextName);
    }

    /**
     * Registers a metrics MBean into the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void register(Logger logger) {
        try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                logger.info("JMX not supported, bean '{}' not registered");
                return;
            }
            mBeanServer.registerMBean(this, name);
            registered = true;
        }
        catch (JMException e) {
            logger.warn("Unable to register the MBean '{}': {}", name, e.getMessage());
        }
    }

    /**
     * Unregisters a metrics MBean from the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void unregister(Logger logger) {
        if (this.name != null && registered) {
            try {
                final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                if (mBeanServer == null) {
                    logger.debug("JMX not supported, bean '{}' not registered");
                    return;
                }
                mBeanServer.unregisterMBean(name);
            }
            catch (JMException e) {
                logger.warn("Unable to unregister the MBean '{}': {}", name, e.getMessage());
            }
        }
    }

    protected ObjectName metricName(String connectorType, String connectorName, String contextName) {
        return metricName(connectorType, connectorName, contextName, new HashMap<>());
    }

    /**
     * Create a JMX metric name for the given metric.
     * @param contextName the name of the context
     * @return the JMX metric name
     */
    protected ObjectName metricName(String connectorType, String connectorName, String contextName,
                                    Map<String, String> labels) {
        Map<String, String> allLabels = new LinkedHashMap<>();
        allLabels.put("context", contextName);
        allLabels.put("server", connectorName);
        allLabels.putAll(labels);

        final String metricName = "debezium." + connectorType.toLowerCase() + ":type=connector-metrics,"
                + allLabels.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(","));
        try {
            return new ObjectName(metricName);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }
}
