/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.core;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.BoundedConcurrentHashMap.Eviction;

/**
 * Measures {@link BoundedConcurrentHashMap#put(Object, Object)} on existing keys, the hot path of
 * write-through dedup caches such as the OpenLineage SMT. The key set fits well within the map
 * capacity, so every operation is a hit. A batch of {@code putCount} puts is timed as a whole:
 * doubling {@code putCount} should roughly double the time, so a superlinear growth between the
 * two data points exposes a per-operation cost that increases with the number of operations.
 */
@Fork(1)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode({ Mode.SingleShotTime })
public class BoundedConcurrentHashMapPerf {

    private static final int CAPACITY = 64;
    private static final int KEY_COUNT = 32;

    @Param({ "100000", "400000" })
    private int putCount;

    @Param({ "LRU", "LIRS" })
    private Eviction eviction;

    private BoundedConcurrentHashMap<Integer, Integer> map;

    @Setup(Level.Iteration)
    public void prepare() {
        map = new BoundedConcurrentHashMap<>(CAPACITY, 16, eviction);
        for (int i = 0; i < KEY_COUNT; i++) {
            map.put(i, i);
        }
    }

    @Benchmark
    public void putExistingKeys(Blackhole blackhole) {
        for (int i = 0; i < putCount; i++) {
            blackhole.consume(map.put(i % KEY_COUNT, i));
        }
    }
}
