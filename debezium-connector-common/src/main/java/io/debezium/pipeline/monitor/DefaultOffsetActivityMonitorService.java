/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * The default implementation of the {@link OffsetActivityMonitorService} contract.
 *
 * The registered monitor is invoked at most once per configured check interval, with the first
 * check occurring after one full interval has elapsed. A non-positive interval disables the
 * service, making {@link #pulse()} a no-op.
 *
 * This implementation is not thread-safe; {@link #register(OffsetActivityMonitor)} and
 * {@link #pulse()} are expected to be invoked from the streaming thread.
 *
 * @author Chris Cranford
 */
public class DefaultOffsetActivityMonitorService implements OffsetActivityMonitorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOffsetActivityMonitorService.class);

    private final ElapsedTimeStrategy elapsedStrategy;

    private OffsetActivityMonitor monitor;

    public DefaultOffsetActivityMonitorService(Duration checkInterval) {
        this(checkInterval, Clock.SYSTEM);
    }

    public DefaultOffsetActivityMonitorService(Duration checkInterval, Clock clock) {
        if (checkInterval == null || checkInterval.isZero() || checkInterval.isNegative()) {
            LOGGER.info("Offset activity monitoring is disabled.");
            this.elapsedStrategy = null;
        }
        else {
            this.elapsedStrategy = ElapsedTimeStrategy.constant(clock, checkInterval);
        }
    }

    @Override
    public void register(OffsetActivityMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void pulse() {
        if (monitor != null && elapsedStrategy != null && elapsedStrategy.hasElapsed()) {
            try {
                monitor.checkForStaleOffsets();
            }
            catch (Exception e) {
                LOGGER.warn("Offset activity check failed, it will be retried at the next interval.", e);
            }
        }
    }
}