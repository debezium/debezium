/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * A thread-safe interner that deduplicates objects by their {@link Object#equals(Object) equals}
 * semantics, similar to {@link String#intern()} but for arbitrary immutable types.
 *
 * <p>Three modes are available, configured per connector via {@link #activate(Mode)}:</p>
 * <ul>
 *   <li>{@link Mode#OFF}: no interning, all calls return the input as-is.</li>
 *   <li>{@link Mode#ON}: each activation creates an isolated pool for the calling thread hierarchy.
 *       Useful to avoid cross-connector interference while still deduplicating within a single connector.</li>
 *   <li>{@link Mode#SHARED}: all activations with this mode share a single JVM-wide pool.
 *       Maximizes deduplication when many connectors track identical schema structures.</li>
 * </ul>
 *
 * <p>Each pool uses weak references so interned instances become eligible for garbage collection
 * once no strong references to them remain.</p>
 *
 * <p>Warning: do not use with mutable objects. The interning contract assumes that
 * objects do not change after being interned.</p>
 */
public final class Interner {

    /**
     * Controls how the interner pools schema objects.
     */
    public enum Mode {
        /** No interning is performed. */
        OFF,
        /** Each connector uses its own isolated intern pool. */
        ON,
        /** All connectors configured with this mode share a single global pool. */
        SHARED;
    }

    private static final Pool SHARED_POOL = new Pool();

    /**
     * InheritableThreadLocal so that connector-spawned threads (e.g. snapshot workers)
     * inherit the pool activated on the connector task thread.
     */
    private static final InheritableThreadLocal<Pool> ACTIVE = new InheritableThreadLocal<>();

    private Interner() {
    }

    /**
     * Activates the given mode on the current thread (and any threads it subsequently spawns).
     * <ul>
     *   <li>{@link Mode#OFF}: disables interning on this thread; returns {@code null}.</li>
     *   <li>{@link Mode#ON}: creates a new isolated pool for this thread; returns its {@link InternerStats}.</li>
     *   <li>{@link Mode#SHARED}: routes interning through the shared global pool; returns its {@link InternerStats}.</li>
     * </ul>
     *
     * @return the active pool's statistics handle, or {@code null} when mode is {@link Mode#OFF}
     */
    public static InternerStats activate(Mode mode) {
        return switch (mode) {
            case OFF -> {
                ACTIVE.remove();
                yield null;
            }
            case ON -> {
                Pool pool = new Pool();
                ACTIVE.set(pool);
                yield pool;
            }
            case SHARED -> {
                ACTIVE.set(SHARED_POOL);
                yield SHARED_POOL;
            }
        };
    }

    /**
     * Returns a canonical instance equal to the given value. If an equal value has already been
     * interned in the active pool and is still strongly reachable, the previously interned instance
     * is returned. Otherwise the given value is stored and returned.
     *
     * <p>When no pool is active (mode is {@link Mode#OFF} or {@link #activate} was never called),
     * the sample is returned as-is.</p>
     *
     * @param sample the value to intern; may be {@code null}
     * @param <T> the type of the value
     * @return the canonical instance, or {@code null} if the input was {@code null}
     */
    public static <T> T intern(T sample) {
        if (sample == null) {
            return null;
        }
        Pool pool = ACTIVE.get();
        if (pool == null) {
            return sample;
        }
        return pool.intern(sample);
    }

    /**
     * Clears all pools (both the active pool and the shared pool).
     * Primarily intended for testing.
     */
    public static void clear() {
        Pool active = ACTIVE.get();
        if (active != null && active != SHARED_POOL) {
            active.clear();
        }
        SHARED_POOL.clear();
    }

    /**
     * Returns the number of entries in the active pool, or in the shared pool if no pool is active.
     */
    static int size() {
        Pool pool = ACTIVE.get();
        return pool != null ? pool.size() : SHARED_POOL.size();
    }

    /**
     * An isolated intern pool backed by a weak-reference map.
     * Hit and miss counts are tracked via {@link LongAdder} for minimal write overhead on the hot path.
     * String values are delegated to {@link String#intern()} and are not counted.
     */
    static final class Pool implements InternerStats {
        private final ConcurrentMap<WeakEntry, WeakEntry> map = new ConcurrentHashMap<>();
        private final ReferenceQueue<Object> queue = new ReferenceQueue<>();
        private final LongAdder hitCount = new LongAdder();
        private final LongAdder missCount = new LongAdder();

        @SuppressWarnings("unchecked")
        <T> T intern(T sample) {
            if (sample instanceof String) {
                return (T) ((String) sample).intern();
            }
            removeStaleEntries();
            WeakEntry newEntry = new WeakEntry(sample, queue);
            while (true) {
                WeakEntry existing = map.putIfAbsent(newEntry, newEntry);
                if (existing == null) {
                    missCount.increment();
                    return sample;
                }
                T canonical = (T) existing.get();
                if (canonical != null) {
                    hitCount.increment();
                    return canonical;
                }
                map.remove(existing, existing);
            }
        }

        void clear() {
            map.clear();
            removeStaleEntries();
        }

        int size() {
            return map.size();
        }

        @Override
        public long hitCount() {
            return hitCount.sum();
        }

        @Override
        public long missCount() {
            return missCount.sum();
        }

        @Override
        public int poolSize() {
            return map.size();
        }

        @SuppressWarnings("unchecked")
        private void removeStaleEntries() {
            Reference<?> ref;
            while ((ref = queue.poll()) != null) {
                map.remove((WeakEntry) ref, (WeakEntry) ref);
            }
        }
    }

    /**
     * A weak-reference wrapper used as both key and value in the intern pool.
     *
     * <p>The {@link #hashCode()} is cached at construction time from the referent, so it remains
     * stable even after the referent is garbage-collected. The {@link #equals(Object)} method
     * uses identity comparison as a fast path (needed for stale entry removal), then falls back
     * to content-based comparison using the referents' {@code equals()} method.</p>
     */
    private static final class WeakEntry extends WeakReference<Object> {
        private final int hashCode;

        WeakEntry(Object referent, ReferenceQueue<Object> queue) {
            super(referent, queue);
            this.hashCode = referent.hashCode();
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof WeakEntry)) {
                return false;
            }
            Object thisRef = this.get();
            Object thatRef = ((WeakEntry) obj).get();
            return thisRef != null && thisRef.equals(thatRef);
        }
    }
}
