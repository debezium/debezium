/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.lang.management.ManagementFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;

/**
 * @author Randall Hauch
 *
 */
abstract class Metrics {

    private final String contextName;
    private ObjectName name;
    
    protected Metrics(String contextName) {
        this.contextName = contextName;
    }
    
    public void register(MySqlTaskContext context, Logger logger) {
        try {
            this.name = context.metricName(this.contextName);
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.registerMBean(this, name);
        } catch (JMException e) {
            logger.warn("Error while register the MBean '{}': {}", name, e.getMessage());
        }
    }
    
    public void unregister(Logger logger) {
        if (this.name != null) {
            try {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                mBeanServer.unregisterMBean(name);
            } catch (JMException e) {
                logger.error("Unable to unregister the MBean '{}'", name);
            } finally {
                this.name = null;
            }
        }
    }
}
