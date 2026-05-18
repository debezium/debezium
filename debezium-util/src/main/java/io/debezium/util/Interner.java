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

/**
 * A thread-safe, static interner that deduplicates objects by their {@link Object#equals(Object) equals}
 * semantics, similar to {@link String#intern()} but for arbitrary immutable types.
 *
 * <p>This implementation uses weak references
 * so that interned instances become eligible for garbage collection once no strong references
 * to them remain. This prevents the memory leaks inherent in a strong-reference pool.</p>
 *
 * <p>This is useful when many equal copies of immutable objects exist in memory, for example
 * when thousands of database schemas share identical table structures.</p>
 *
 * <p>Warning: do not use with mutable objects. The interning contract assumes that
 * objects do not change after being interned.</p>
 */
public final class Interner {

    private static final ConcurrentMap<WeakEntry, WeakEntry> MAP = new ConcurrentHashMap<>();
    private static final ReferenceQueue<Object> QUEUE = new ReferenceQueue<>();
    private static boolean enabled = false;

    private Interner() {
    }

    /**
     * Enables or disables the interner.
     * When disabled, {@link #intern(Object)} returns the input value without deduplication.
     *
     * @param enabled {@code true} to enable interning, {@code false} to bypass it
     */
    public static void setEnabled(boolean enabled) {
        Interner.enabled = enabled;
    }

    /**
     * Returns a canonical instance equal to the given value. If an equal value has already been
     * interned and is still strongly reachable, the previously interned instance is returned.
     * Otherwise, the given value is stored and returned.
     *
     * <p>When the interner is {@link #setEnabled(boolean) disabled}, the sample is returned as-is.</p>
     *
     * <p>Note that a previously interned instance may be garbage-collected if no strong references
     * to it remain, in which case a subsequent call with an equal value will intern the new instance.</p>
     *
     * @param sample the value to intern; may be {@code null}
     * @param <T> the type of the value
     * @return the canonical instance, or {@code null} if the input was {@code null}
     */
    @SuppressWarnings("unchecked")
    public static <T> T intern(T sample) {
        if (!enabled || sample == null) {
            return sample;
        }
        if (sample instanceof String) {
            return (T) ((String) sample).intern();
        }
        removeStaleEntries();
        WeakEntry newEntry = new WeakEntry(sample, QUEUE);
        while (true) {
            WeakEntry existing = MAP.putIfAbsent(newEntry, newEntry);
            if (existing == null) {
                return sample;
            }
            T canonical = (T) existing.get();
            if (canonical != null) {
                return canonical;
            }
            MAP.remove(existing, existing);
        }
    }

    /**
     * Returns the number of entries currently held in the pool. This includes entries whose
     * referents may have been garbage-collected but not yet expunged.
     *
     * @return the pool size
     */
    static int size() {
        return MAP.size();
    }

    /**
     * Removes all entries from the pool.
     */
    public static void clear() {
        MAP.clear();
        removeStaleEntries();
    }

    @SuppressWarnings("unchecked")
    private static void removeStaleEntries() {
        Reference<?> ref;
        while ((ref = QUEUE.poll()) != null) {
            MAP.remove((WeakEntry) ref, (WeakEntry) ref);
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

        WeakEntry(Object referent, ReferenceQueue<Object> QUEUE) {
            super(referent, QUEUE);
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
