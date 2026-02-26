/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

import io.debezium.annotation.NotThreadSafe;

/**
 * A simple grow-only counter that maintains a single changing value by tracking the positive changes to the value. It is
 * inspired by the conflict-free replicated data type (CRDT) G Counter.
 */
@NotThreadSafe
public interface GCounter extends GCount {
    /**
     * Increment the counter and get the result.
     *
     * @return this instance so methods can be chained together; never null
     */
    GCounter increment();

    /**
     * Increment the counter and get the result.
     *
     * @return the current result after incrementing
     */
    long incrementAndGet();

    /**
     * Increment the counter and get the result.
     *
     * @return the current result before incrementing
     */
    long getAndIncrement();

    /**
     * Merge the supplied counter into this counter.
     *
     * @param other the other counter to merge into this one; may be null
     * @return this counter so that methods can be chained
     */
    GCounter merge(Count other);
}
