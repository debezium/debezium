/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import io.debezium.common.annotation.Incubating;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.service.Service;

/**
 * A service that coordinates the periodic evaluation of a connector's {@link OffsetActivityMonitor}.
 *
 * The service owns the check cadence; connectors are expected to invoke
 * {@link #pulse(Partition, OffsetContext)} once per streaming loop iteration with the partition
 * and offsets being streamed, and the registered monitor is only consulted when the configured
 * check interval has elapsed.
 *
 * For connectors that stream multiple partitions, the monitor is consulted with the partition
 * that was being streamed when the check interval elapsed; each partition's state is therefore
 * examined at least once per interval rather than exactly once per interval.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OffsetActivityMonitorService extends Service {
    /**
     * Registers the connector-specific offset activity monitor. Registering a new monitor
     * replaces any previously registered instance. The monitor's type parameters must match
     * the partition and offsets the connector supplies to {@link #pulse(Partition, OffsetContext)}.
     *
     * @param monitor the offset activity monitor, should not be {@code null}
     */
    <P extends Partition, O extends OffsetContext> void register(OffsetActivityMonitor<P, O> monitor);

    /**
     * Signals that the streaming loop has performed an iteration. When the configured check
     * interval has elapsed, the registered monitor's
     * {@link OffsetActivityMonitor#checkForStaleOffsets(Partition, OffsetContext)} is invoked
     * with the given partition and offsets; otherwise this is a no-op. This is also a no-op
     * when no monitor has been registered or when the service is disabled.
     *
     * @param partition the partition currently being streamed, should not be {@code null}
     * @param offsetContext the partition's offsets, should not be {@code null}
     */
    <P extends Partition, O extends OffsetContext> void pulse(P partition, O offsetContext);
}