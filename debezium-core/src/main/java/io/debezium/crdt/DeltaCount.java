/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

/**
 * A {@link Count} that also tracks changes to the value within the last interval.
 */
public interface DeltaCount extends PNCount {

    /**
     * Get the changes in the current value during the last interval.
     *
     * @return the changes in the value during the last interval; never null
     */
    PNCount getChanges();

    /**
     * Determine if there are any changes in this count.
     * @return {@code true} if there are non-zero {@link #getChanges() changes}, or {@code false} otherwise
     */
    default boolean hasChanges() {
        PNCount changes = getChanges();
        return changes.getIncrement() != 0 || changes.getDecrement() != 0;
    }

    /**
     * Get the value of this count prior to the {@link #getChanges() changes}.
     * @return the prior count; never null
     */
    Count getPriorCount();
}
