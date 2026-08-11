/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;

/**
 * A service that coordinates the periodic evaluation of a connector's {@link OffsetActivityMonitor}.
 *
 * The service owns the check cadence; connectors are expected to invoke {@link #pulse()} once per
 * streaming loop iteration, and the registered monitor is only consulted when the configured
 * check interval has elapsed.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OffsetActivityMonitorService extends Service {
    /**
     * Registers the connector-specific offset activity monitor. Registering a new monitor
     * replaces any previously registered instance.
     *
     * @param monitor the offset activity monitor, should not be {@code null}
     */
    void register(OffsetActivityMonitor monitor);

    /**
     * Signals that the streaming loop has performed an iteration. When the configured check
     * interval has elapsed, the registered monitor's {@link OffsetActivityMonitor#checkForStaleOffsets()}
     * is invoked; otherwise this is a no-op. This is also a no-op when no monitor has been
     * registered or when the service is disabled.
     */
    void pulse();
}