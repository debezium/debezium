/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metrics;

import java.lang.management.ManagementFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;

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
    public final void unregister(Logger logger) {
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

    /**
     * Create a JMX metric name for the given metric.
     * @param contextName the name of the context
     * @return the JMX metric name
     * @throws MalformedObjectNameException if the name is invalid
     */
    public ObjectName metricName(String connectorType, String connectorName, String contextName) {
        final String metricName = "debezium." + connectorType.toLowerCase() + ":type=connector-metrics,context=" + contextName + ",server=" + connectorName;
        try {
            return new ObjectName(metricName);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }
}
