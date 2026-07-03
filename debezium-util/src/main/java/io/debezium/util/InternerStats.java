/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

/**
 * Read-only view of an {@link Interner} pool's runtime statistics.
 * Returned by {@link Interner#activate(Interner.Mode)} when interning is enabled,
 * and exposed via JMX to monitor deduplication effectiveness.
 */
public interface InternerStats {

    /** Number of times {@code intern()} returned an already-cached canonical instance. */
    long hitCount();

    /** Number of times {@code intern()} stored a new instance in the pool. */
    long missCount();

    /** Current number of entries held in the pool (including entries pending GC expiry). */
    int poolSize();
}
