/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * The default implementation of the {@link OffsetActivityMonitorService} contract.
 * <p>
 * The registered monitor is invoked at most once per configured check interval, with the first
 * check occurring after one full interval has elapsed. When the monitor reports a
 * {@link StaleOffsetsResult.Stale} result, its message is logged as a warning. A non-positive
 * interval disables the service, making {@link #pulse(Partition, OffsetContext)} a no-op.
 * <p>
 * This implementation is not thread-safe; {@link #register(OffsetActivityMonitor)} and
 * {@link #pulse(Partition, OffsetContext)} are expected to be invoked from the streaming thread.
 *
 * @author Chris Cranford
 */
public class DefaultOffsetActivityMonitorService implements OffsetActivityMonitorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultOffsetActivityMonitorService.class);

    private final ElapsedTimeStrategy elapsedStrategy;

    private OffsetActivityMonitor<Partition, OffsetContext> monitor;

    public DefaultOffsetActivityMonitorService(Duration checkInterval) {
        this(checkInterval, Clock.SYSTEM);
    }

    /**
     * Creates a disabled service instance whose pulses are no-ops.
     */
    public static DefaultOffsetActivityMonitorService disabled() {
        return new DefaultOffsetActivityMonitorService(null);
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
    @SuppressWarnings("unchecked")
    public <P extends Partition, O extends OffsetContext> void register(OffsetActivityMonitor<P, O> monitor) {
        // Safe: the monitor is registered by the same streaming source that pulses with matching types
        this.monitor = (OffsetActivityMonitor<Partition, OffsetContext>) monitor;
    }

    @Override
    public <P extends Partition, O extends OffsetContext> void pulse(P partition, O offsetContext) {
        if (monitor != null && elapsedStrategy != null && elapsedStrategy.hasElapsed()) {
            try {
                if (monitor.checkForStaleOffsets(partition, offsetContext) instanceof StaleOffsetsResult.Stale stale) {
                    LOGGER.warn("{}", stale.message());
                }
            }
            catch (Exception e) {
                LOGGER.warn("Offset activity check failed, it will be retried at the next interval.", e);
            }
        }
    }
}