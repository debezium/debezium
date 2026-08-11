/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import io.debezium.common.annotation.Incubating;

/**
 * Contract used to define connector-specific offset state for staleness.
 * Implementations are stateful and are invoked from the streaming main loop.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OffsetActivityMonitor {
    /**
     * Checks for stale offsets
     */
    void checkForStaleOffsets();
}
