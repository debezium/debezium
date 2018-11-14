/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.util.Clock;

/**
 * Base for metrics implementations.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public abstract class Metrics implements DataChangeEventListener, ChangeEventSourceMetricsMXBean {

    protected final AtomicLong totalNumberOfEventsSeen = new AtomicLong();
    protected final AtomicLong lastEventTimestamp = new AtomicLong(-1);

    private final String contextName;
    protected final Clock clock;
    private final CdcSourceTaskContext taskContext;
    private volatile ObjectName name;

    protected <T extends CdcSourceTaskContext> Metrics(T taskContext, String contextName) {
        this.contextName = contextName;
        this.taskContext = taskContext;
        this.clock = taskContext.getClock();
    }

    /**
     * Registers a metrics MBean into the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized <T extends CdcSourceTaskContext> void register(Logger logger) {
        try {
            name = taskContext.metricName(this.contextName);
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.registerMBean(this, name);
        }
        catch (JMException e) {
            logger.warn("Error while register the MBean '{}': {}", name, e.getMessage());
        }
    }

    /**
     * Unregisters a metrics MBean from the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public final void unregister(Logger logger) {
        if (this.name != null) {
            try {
                final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                mBeanServer.unregisterMBean(name);
            }
            catch (JMException e) {
                logger.error("Unable to unregister the MBean '{}'", name);
            }
            finally {
                this.name = null;
            }
        }
    }

    @Override
    public void onEvent() {
        totalNumberOfEventsSeen.incrementAndGet();
        lastEventTimestamp.set(clock.currentTimeInMillis());
    }

    // TODO DBZ-978
    @Override
    public String getLastEvent() {
        return "not implemented";
    }

    @Override
    public long getMilliSecondsSinceLastEvent() {
        return (lastEventTimestamp.get() == -1) ? -1 : (clock.currentTimeInMillis() - lastEventTimestamp.get());
    }

    @Override
    public long getTotalNumberOfEventsSeen() {
        return totalNumberOfEventsSeen.get();
    }

    @Override
    public void reset() {
        totalNumberOfEventsSeen.set(0);
        lastEventTimestamp.set(-1);
    }
}
