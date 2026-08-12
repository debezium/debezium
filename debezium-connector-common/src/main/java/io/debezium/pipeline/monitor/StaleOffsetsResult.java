/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.monitor;

import java.util.Objects;

import io.debezium.common.annotation.Incubating;

/**
 * The outcome of an {@link OffsetActivityMonitor} staleness check.
 *
 * The result is either {@link Stale}, carrying a human-readable message that describes why the
 * offsets are considered stale, or {@link Fresh}, indicating the offsets have progressed since
 * the last check. Instances are created via the {@link #stale(String)} and {@link #fresh()}
 * factory methods.
 *
 * @author Chris Cranford
 */
@Incubating
public sealed interface StaleOffsetsResult permits StaleOffsetsResult.Stale, StaleOffsetsResult.Fresh {
    /**
     * Indicates the offsets have not progressed since the last check.
     *
     * @param message a human-readable description of why the offsets are considered stale,
     *         suitable for logging; never {@code null}
     */
    record Stale(String message) implements StaleOffsetsResult {
        public Stale {
            Objects.requireNonNull(message, "The stale message should not be null");
        }
    }

    /**
     * Indicates the offsets have progressed since the last check.
     */
    record Fresh() implements StaleOffsetsResult {
    }

    /**
     * Creates a result indicating the offsets are stale.
     *
     * @param message a human-readable description of why the offsets are considered stale,
     *         suitable for logging; should not be {@code null}
     * @return the stale result, never {@code null}
     */
    static StaleOffsetsResult stale(String message) {
        return new Stale(message);
    }

    /**
     * Creates a result indicating the offsets have progressed since the last check.
     *
     * @return the fresh result, never {@code null}
     */
    static StaleOffsetsResult fresh() {
        return new Fresh();
    }
}
