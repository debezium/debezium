/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import io.debezium.common.annotation.Incubating;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

/**
 * Contract used to define connector-specific offset state for staleness.
 * Implementations are stateful and are invoked from the streaming main loop.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OffsetActivityMonitor<P extends Partition, O extends OffsetContext> {
    /**
     * Checks for stale offsets.
     *
     * @param partition the partition currently being streamed, should not be {@code null}
     * @param offsetContext the partition's offsets, should not be {@code null}
     */
    void checkForStaleOffsets(P partition, O offsetContext);
}