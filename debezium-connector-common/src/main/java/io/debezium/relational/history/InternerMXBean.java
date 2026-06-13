/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

/**
 * JMX metrics for the schema object interner.
 * Exposed under {@code debezium.<connector>:type=connector-metrics,context=interner,server=<logical-name>}.
 *
 * <p>Hit and miss counts cover non-string objects only (columns, attributes, lists).
 * String values are delegated to the JVM string pool and are not counted here.</p>
 */
public interface InternerMXBean {

    /** Number of times {@code intern()} returned an already-cached canonical instance. */
    long getHitCount();

    /** Number of times {@code intern()} stored a new instance in the pool. */
    long getMissCount();

    /**
     * Current number of entries held in the pool.
     * Includes entries whose referents may have been garbage-collected but not yet expunged.
     */
    int getPoolSize();

    /**
     * Hit ratio as a percentage ({@code hitCount / (hitCount + missCount) * 100}),
     * or {@code 0} if no calls have been recorded yet.
     */
    double getHitRatioPercent();
}
