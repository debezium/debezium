/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

import io.debezium.annotation.NotThreadSafe;

/**
 * A simple counter that maintains a single changing value by separately tracking the positive and negative changes. It is
 * inspired by the conflict-free replicated data type (CRDT) P-N Counter. The semantics ensure the value converges toward the
 * global number of increments minus the number of decrements. The global total can be calculated by {@link #merge(Count) merging}
 * all the replicated instances, without regard to order of merging.
 */
@NotThreadSafe
public interface PNCounter extends PNCount, GCounter {

    /**
     * Increment the counter and get the result.
     *
     * @return this instance so methods can be chained together; never null
     */
    @Override
    PNCounter increment();

    /**
     * Decrement the counter and get the result.
     *
     * @return this instance so methods can be chained together; never null
     */
    PNCounter decrement();

    /**
     * Decrement the counter and get the result.
     *
     * @return the current result after decrementing
     */
    long decrementAndGet();

    /**
     * Decrement the counter and get the result.
     *
     * @return the current result before decrementing
     */
    long getAndDecrement();

    /**
     * Merge the supplied counter into this counter.
     *
     * @param other the other counter to merge into this one; may be null
     * @return this counter so that methods can be chained
     */
    @Override
    PNCounter merge(Count other);
}
