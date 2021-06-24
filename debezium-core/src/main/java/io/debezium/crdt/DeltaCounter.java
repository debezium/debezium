/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

import io.debezium.annotation.NotThreadSafe;

/**
 * A simple counter that maintains a single changing value by separately tracking the positive and negative changes, and by
 * tracking recent {@link #getChanges() changes} in this value since last {@link #reset() reset}. It is
 * inspired by the conflict-free replicated data type (CRDT) P-N Counter. The semantics ensure the value converges toward the
 * global number of increments minus the number of decrements. The global total can be calculated by {@link #merge(Count) merging}
 * all the replicated instances, without regard to order of merging.
 */
@NotThreadSafe
public interface DeltaCounter extends PNCounter, DeltaCount {
    /**
     * Increment the counter and get the result.
     *
     * @return this instance so methods can be chained together; never null
     */
    @Override
    DeltaCounter increment();

    /**
     * Decrement the counter and get the result.
     *
     * @return this instance so methods can be chained together; never null
     */
    @Override
    DeltaCounter decrement();

    /**
     * Merge the supplied counter into this counter.
     *
     * @param other the other counter to merge into this one; may be null
     * @return this counter so that methods can be chained
     */
    @Override
    DeltaCounter merge(Count other);

    /**
     * Start a new interval and reset the {@link #getChanges()} to initial values.
     */
    void reset();
}
