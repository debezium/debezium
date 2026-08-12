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
 * Contract used to examine connector-specific offset state for staleness.
 * <p>
 * Implementations compare the supplied offsets against the state captured during the previous
 * check and report the outcome as a {@link StaleOffsetsResult}. Implementations are stateful
 * and are invoked from the streaming main loop by the {@link OffsetActivityMonitorService}.
 *
 * @author Chris Cranford
 */
@Incubating
public interface OffsetActivityMonitor<P extends Partition, O extends OffsetContext> {
    /**
     * Checks whether the given partition's offsets have progressed since the last check.
     *
     * @param partition the partition currently being streamed, should not be {@code null}
     * @param offsetContext the partition's offsets, should not be {@code null}
     * @return {@link StaleOffsetsResult.Stale} with a descriptive message when the offsets have
     *         not progressed since the last check, {@link StaleOffsetsResult.Fresh} otherwise;
     *         never {@code null}
     */
    StaleOffsetsResult checkForStaleOffsets(P partition, O offsetContext);
}