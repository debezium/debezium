/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.util.Collect;

/**
 * Base for metrics implementations.
 *
 * @author Jiri Pechanec
 */
@ThreadSafe
public abstract class Metrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(Metrics.class);

    // Total 1 minute attempting to retry metrics registration in case of errors
    private static final int REGISTRATION_RETRIES = 12;
    private static final Duration REGISTRATION_RETRY_DELAY = Duration.ofSeconds(5);

    private final ObjectName name;
    private volatile boolean registered = false;

    protected Metrics(CdcSourceTaskContext taskContext, String contextName) {
        this.name = metricName(taskContext.getConnectorType(), taskContext.getConnectorName(), contextName);
    }

    protected Metrics(CdcSourceTaskContext taskContext, Map<String, String> tags) {
        this.name = metricName(taskContext.getConnectorType(), tags);
    }

    protected Metrics(CommonConnectorConfig connectorConfig, String contextName, boolean multiPartitionMode) {
        String connectorType = connectorConfig.getContextName();
        String connectorName = connectorConfig.getLogicalName();
        if (multiPartitionMode) {
            this.name = metricName(connectorType, Collect.linkMapOf(
                    "server", connectorName,
                    "task", connectorConfig.getTaskId(),
                    "context", contextName));
        }
        else {
            this.name = metricName(connectorType, connectorName, contextName);
        }
    }

    /**
     * Registers a metrics MBean into the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void register() {
        try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.info("JMX not supported, bean '{}' not registered", name);
                return;
            }
            // During connector restarts it is possible that Kafka Connect does not manage
            // the lifecycle perfectly. In that case it is possible the old metric MBean is still present.
            // There will be multiple attempts executed to register new MBean.
            for (int attempt = 1; attempt <= REGISTRATION_RETRIES; attempt++) {
                try {
                    mBeanServer.registerMBean(this, name);
                    break;
                }
                catch (InstanceAlreadyExistsException e) {
                    if (attempt <= REGISTRATION_RETRIES) {
                        LOGGER.warn(
                                "Unable to register metrics as an old set with the same name exists, retrying in {} (attempt {} out of {})",
                                REGISTRATION_RETRY_DELAY, attempt, REGISTRATION_RETRIES);
                    }
                    else {
                        LOGGER.error("Failed to register metrics MBean, metrics will not be available");
                    }
                }
            }
            // If the old metrics MBean is present then the connector will try to unregister it
            // upon shutdown.
            registered = true;
        }
        catch (JMException e) {
            throw new RuntimeException("Unable to register the MBean '" + name + "'", e);
        }
    }

    /**
     * Unregisters a metrics MBean from the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void unregister() {
        if (this.name != null && registered) {
            try {
                final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                if (mBeanServer == null) {
                    LOGGER.debug("JMX not supported, bean '{}' not registered", name);
                    return;
                }
                try {
                    mBeanServer.unregisterMBean(name);
                }
                catch (InstanceNotFoundException e) {
                    LOGGER.info("Unable to unregister metrics MBean '{}' as it was not found", name);
                }
                registered = false;
            }
            catch (JMException e) {
                throw new RuntimeException("Unable to unregister the MBean '" + name + "'", e);
            }
        }
    }

    protected ObjectName metricName(String connectorType, String connectorName, String contextName) {
        return metricName(connectorType, Collect.linkMapOf("context", contextName, "server", connectorName));
    }

    /**
     * Create a JMX metric name for the given metric.
     * @return the JMX metric name
     */
    protected ObjectName metricName(String connectorType, Map<String, String> tags) {
        final String metricName = "debezium." + connectorType.toLowerCase() + "connector-metrics,"
                + tags.entrySet().stream()
                        .map(e -> e.getKey() + "=" + Sanitizer.jmxSanitize(e.getValue()))
                        .collect(Collectors.joining(","));
        try {
            return new ObjectName(metricName);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }
}
