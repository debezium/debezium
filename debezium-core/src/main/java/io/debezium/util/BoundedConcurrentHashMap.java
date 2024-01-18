/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Modified for https://jira.jboss.org/jira/browse/ISPN-299
 * Includes ideas described in http://portal.acm.org/citation.cfm?id=1547428
 *
 */
package io.debezium.util;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A hash table supporting full concurrency of retrievals and
 * adjustable expected concurrency for updates. This class obeys the
 * same functional specification as {@link java.util.Hashtable}, and
 * includes versions of methods corresponding to each method of
 * <tt>Hashtable</tt>. However, even though all operations are
 * thread-safe, retrieval operations do <em>not</em> entail locking,
 * and there is <em>not</em> any support for locking the entire table
 * in a way that prevents all access.  This class is fully
 * interoperable with <tt>Hashtable</tt> in programs that rely on its
 * thread safety but not on its synchronization details.
 * <p/>
 * <p> Retrieval operations (including <tt>get</tt>) generally do not
 * block, so may overlap with update operations (including
 * <tt>put</tt> and <tt>remove</tt>). Retrievals reflect the results
 * of the most recently <em>completed</em> update operations holding
 * upon their onset.  For aggregate operations such as <tt>putAll</tt>
 * and <tt>clear</tt>, concurrent retrievals may reflect insertion or
 * removal of only some entries.  Similarly, Iterators and
 * Enumerations return elements reflecting the state of the hash table
 * at some point at or since the creation of the iterator/enumeration.
 * They do <em>not</em> throw {@link java.util.ConcurrentModificationException}.
 * However, iterators are designed to be used by only one thread at a time.
 * <p/>
 * <p> The allowed concurrency among update operations is guided by
 * the optional <tt>concurrencyLevel</tt> constructor argument
 * (default <tt>16</tt>), which is used as a hint for internal sizing.  The
 * table is internally partitioned to try to permit the indicated
 * number of concurrent updates without contention. Because placement
 * in hash tables is essentially random, the actual concurrency will
 * vary.  Ideally, you should choose a value to accommodate as many
 * threads as will ever concurrently modify the table. Using a
 * significantly higher value than you need can waste space and time,
 * and a significantly lower value can lead to thread contention. But
 * overestimates and underestimates within an order of magnitude do
 * not usually have much noticeable impact. A value of one is
 * appropriate when it is known that only one thread will modify and
 * all others will only read. Also, resizing this or any other kind of
 * hash table is a relatively slow operation, so, when possible, it is
 * a good idea to provide estimates of expected table sizes in
 * constructors.
 * <p/>
 * <p>This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces.
 * <p/>
 * <p>This class is copied from Hibernate (which took it from Infinispan),
 * and was originally written
 * by Doug Lea with assistance from members of JCP JSR-166 Expert Group and
 * released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain</p>
 * <p/>
 * <p/>
 * <p> Like {@link java.util.Hashtable} but unlike {@link HashMap}, this class
 * does <em>not</em> allow <tt>null</tt> to be used as a key or value.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Doug Lea
 */
public class BoundedConcurrentHashMap<K, V> extends AbstractMap<K, V>
        implements ConcurrentMap<K, V>, Serializable {
    private static final long serialVersionUID = 7249069246763182397L;

    /*
     * The basic strategy is to subdivide the table among Segments,
     * each of which itself is a concurrently readable hash table.
     */

    /* ---------------- Constants -------------- */

    /**
     * The default initial capacity for this table,
     * used when not otherwise specified in a constructor.
     */
    static final int DEFAULT_MAXIMUM_CAPACITY = 512;

    /**
     * The default load factor for this table, used when not
     * otherwise specified in a constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The default concurrency level for this table, used when not
     * otherwise specified in a constructor.
     */
    static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly
     * specified by either of the constructors with arguments.  MUST
     * be a power of two <= 1<<30 to ensure that entries are indexable
     * using ints.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound
     * constructor arguments.
     */
    static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /**
     * Number of unsynchronized retries in size and containsValue
     * methods before resorting to locking. This is used to avoid
     * unbounded retries if tables undergo continuous modification
     * which would make it impossible to obtain an accurate result.
     */
    static final int RETRIES_BEFORE_LOCK = 2;

    /* ---------------- Fields -------------- */

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    final Segment<K, V>[] segments;

    transient Set<K> keySet;
    transient Set<Map.Entry<K, V>> entrySet;
    transient Collection<V> values;

    /* ---------------- Small Utilities -------------- */

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += h << 15 ^ 0xffffcd7d;
        h ^= h >>> 10;
        h += h << 3;
        h ^= h >>> 6;
        h += (h << 2) + (h << 14);
        return h ^ h >>> 16;
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash the hash code for the key
     *
     * @return the segment
     */
    final Segment<K, V> segmentFor(int hash) {
        return segments[hash >>> segmentShift & segmentMask];
    }

    /* ---------------- Inner Classes -------------- */

    /**
     * ConcurrentHashMap list entry. Note that this is never exported
     * out as a user-visible Map.Entry.
     * <p/>
     * Because the value field is volatile, not final, it is legal wrt
     * the Java Memory Model for an unsynchronized reader to see null
     * instead of initial value when read via a data race.  Although a
     * reordering leading to this is not likely to ever actually
     * occur, the Segment.readValueUnderLock method is used as a
     * backup in case a null (pre-initialized) value is ever seen in
     * an unsynchronized access method.
     */
    private static class HashEntry<K, V> {
        final K key;
        final int hash;
        volatile V value;
        final HashEntry<K, V> next;

        HashEntry(K key, int hash, HashEntry<K, V> next, V value) {
            this.key = key;
            this.hash = hash;
            this.next = next;
            this.value = value;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = result * 31 + hash;
            result = result * 31 + key.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            // HashEntry is internal class, never leaks out of CHM, hence slight optimization
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            HashEntry<?, ?> other = (HashEntry<?, ?>) o;
            return hash == other.hash && key.equals(other.key);
        }

        @SuppressWarnings("unchecked")
        static <K, V> HashEntry<K, V>[] newArray(int i) {
            return new HashEntry[i];
        }
    }

    private enum Recency {
        HIR_RESIDENT,
        LIR_RESIDENT,
        HIR_NONRESIDENT
    }

    public enum Eviction {
        NONE {
            @Override
            public <K, V> EvictionPolicy<K, V> make(Segment<K, V> s, int capacity, float lf) {
                return new NullEvictionPolicy<K, V>();
            }
        },
        LRU {
            @Override
            public <K, V> EvictionPolicy<K, V> make(Segment<K, V> s, int capacity, float lf) {
                return new LRU<K, V>(s, capacity, lf, capacity * 10, lf);
            }
        },
        LIRS {
            @Override
            public <K, V> EvictionPolicy<K, V> make(Segment<K, V> s, int capacity, float lf) {
                return new LIRS<K, V>(s, capacity, capacity * 10, lf);
            }
        };

        abstract <K, V> EvictionPolicy<K, V> make(Segment<K, V> s, int capacity, float lf);
    }

    public interface EvictionListener<K, V> {
        void onEntryEviction(Map<K, V> evicted);

        void onEntryChosenForEviction(V internalCacheEntry);
    }

    static final class NullEvictionListener<K, V> implements EvictionListener<K, V> {
        @Override
        public void onEntryEviction(Map<K, V> evicted) {
            // Do nothing.
        }

        @Override
        public void onEntryChosenForEviction(V internalCacheEntry) {
            // Do nothing.
        }
    }

    public interface EvictionPolicy<K, V> {

        int MAX_BATCH_SIZE = 64;

        HashEntry<K, V> createNewEntry(K key, int hash, HashEntry<K, V> next, V value);

        /**
         * Invokes eviction policy algorithm and returns set of evicted entries.
         * <p/>
         * <p/>
         * Set cannot be null but could possibly be an empty set.
         *
         * @return set of evicted entries.
         */
        Set<HashEntry<K, V>> execute();

        /**
         * Invoked to notify EvictionPolicy implementation that there has been an attempt to access
         * an entry in Segment, however that entry was not present in Segment.
         *
         * @param e accessed entry in Segment
         *
         * @return non null set of evicted entries.
         */
        Set<HashEntry<K, V>> onEntryMiss(HashEntry<K, V> e);

        /**
         * Invoked to notify EvictionPolicy implementation that an entry in Segment has been
         * accessed. Returns true if batching threshold has been reached, false otherwise.
         * <p/>
         * Note that this method is potentially invoked without holding a lock on Segment.
         *
         * @param e accessed entry in Segment
         *
         * @return true if batching threshold has been reached, false otherwise.
         */
        boolean onEntryHit(HashEntry<K, V> e);

        /**
         * Invoked to notify EvictionPolicy implementation that an entry e has been removed from
         * Segment.
         *
         * @param e removed entry in Segment
         */
        void onEntryRemove(HashEntry<K, V> e);

        /**
         * Invoked to notify EvictionPolicy implementation that all Segment entries have been
         * cleared.
         */
        void clear();

        /**
         * Returns type of eviction algorithm (strategy).
         *
         * @return type of eviction algorithm
         */
        Eviction strategy();

        /**
         * Returns true if batching threshold has expired, false otherwise.
         * <p/>
         * Note that this method is potentially invoked without holding a lock on Segment.
         *
         * @return true if batching threshold has expired, false otherwise.
         */
        boolean thresholdExpired();
    }

    static class NullEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

        @Override
        public void clear() {
            // Do nothing.
        }

        @Override
        public Set<HashEntry<K, V>> execute() {
            return Collections.emptySet();
        }

        @Override
        public boolean onEntryHit(HashEntry<K, V> e) {
            return false;
        }

        @Override
        public Set<HashEntry<K, V>> onEntryMiss(HashEntry<K, V> e) {
            return Collections.emptySet();
        }

        @Override
        public void onEntryRemove(HashEntry<K, V> e) {
            // Do nothing.
        }

        @Override
        public boolean thresholdExpired() {
            return false;
        }

        @Override
        public Eviction strategy() {
            return Eviction.NONE;
        }

        @Override
        public HashEntry<K, V> createNewEntry(K key, int hash, HashEntry<K, V> next, V value) {
            return new HashEntry<K, V>(key, hash, next, value);
        }
    }

    static final class LRU<K, V> extends LinkedHashMap<HashEntry<K, V>, V> implements EvictionPolicy<K, V> {

        /**
         * The serialVersionUID
         */
        private static final long serialVersionUID = -7645068174197717838L;

        private final ConcurrentLinkedQueue<HashEntry<K, V>> accessQueue;
        private final Segment<K, V> segment;
        private final int maxBatchQueueSize;
        private final int trimDownSize;
        private final float batchThresholdFactor;
        private final Set<HashEntry<K, V>> evicted;

        public LRU(Segment<K, V> s, int capacity, float lf, int maxBatchSize, float batchThresholdFactor) {
            super(capacity, lf, true);
            this.segment = s;
            this.trimDownSize = capacity;
            this.maxBatchQueueSize = maxBatchSize > MAX_BATCH_SIZE ? MAX_BATCH_SIZE : maxBatchSize;
            this.batchThresholdFactor = batchThresholdFactor;
            this.accessQueue = new ConcurrentLinkedQueue<HashEntry<K, V>>();
            this.evicted = new HashSet<HashEntry<K, V>>();
        }

        @Override
        public Set<HashEntry<K, V>> execute() {
            Set<HashEntry<K, V>> evictedCopy = new HashSet<HashEntry<K, V>>();
            for (HashEntry<K, V> e : accessQueue) {
                put(e, e.value);
            }
            evictedCopy.addAll(evicted);
            accessQueue.clear();
            evicted.clear();
            return evictedCopy;
        }

        @Override
        public Set<HashEntry<K, V>> onEntryMiss(HashEntry<K, V> e) {
            put(e, e.value);
            if (!evicted.isEmpty()) {
                Set<HashEntry<K, V>> evictedCopy = new HashSet<HashEntry<K, V>>();
                evictedCopy.addAll(evicted);
                evicted.clear();
                return evictedCopy;
            }
            else {
                return Collections.emptySet();
            }
        }

        /*
         * Invoked without holding a lock on Segment
         */
        @Override
        public boolean onEntryHit(HashEntry<K, V> e) {
            accessQueue.add(e);
            return accessQueue.size() >= maxBatchQueueSize * batchThresholdFactor;
        }

        /*
         * Invoked without holding a lock on Segment
         */
        @Override
        public boolean thresholdExpired() {
            return accessQueue.size() >= maxBatchQueueSize;
        }

        @Override
        public void onEntryRemove(HashEntry<K, V> e) {
            remove(e);
            // we could have multiple instances of e in accessQueue; remove them all
            while (accessQueue.remove(e)) {
                continue;
            }
        }

        @Override
        public void clear() {
            super.clear();
            accessQueue.clear();
        }

        @Override
        public Eviction strategy() {
            return Eviction.LRU;
        }

        protected boolean isAboveThreshold() {
            return size() > trimDownSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<HashEntry<K, V>, V> eldest) {
            boolean aboveThreshold = isAboveThreshold();
            if (aboveThreshold) {
                HashEntry<K, V> evictedEntry = eldest.getKey();
                segment.evictionListener.onEntryChosenForEviction(evictedEntry.value);
                segment.remove(evictedEntry.key, evictedEntry.hash, null);
                evicted.add(evictedEntry);
            }
            return aboveThreshold;
        }

        @Override
        public HashEntry<K, V> createNewEntry(K key, int hash, HashEntry<K, V> next, V value) {
            return new HashEntry<K, V>(key, hash, next, value);
        }
    }

    /**
     * Adapted to Infinispan BoundedConcurrentHashMap using LIRS implementation ideas from Charles Fry (fry@google.com)
     * See http://code.google.com/p/concurrentlinkedhashmap/source/browse/trunk/src/test/java/com/googlecode/concurrentlinkedhashmap/caches/LirsMap.java
     * for original sources
     */
    private static final class LIRSHashEntry<K, V> extends HashEntry<K, V> {

        // LIRS stack S
        private LIRSHashEntry<K, V> previousInStack;
        private LIRSHashEntry<K, V> nextInStack;

        // LIRS queue Q
        private LIRSHashEntry<K, V> previousInQueue;
        private LIRSHashEntry<K, V> nextInQueue;
        volatile Recency state;

        LIRS<K, V> owner;

        LIRSHashEntry(LIRS<K, V> owner, K key, int hash, HashEntry<K, V> next, V value) {
            super(key, hash, next, value);
            this.owner = owner;
            this.state = Recency.HIR_RESIDENT;

            // initially point everything back to self
            this.previousInStack = this;
            this.nextInStack = this;
            this.previousInQueue = this;
            this.nextInQueue = this;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = result * 31 + hash;
            result = result * 31 + key.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            // HashEntry is internal class, never leaks out of CHM, hence slight optimization
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            HashEntry<?, ?> other = (HashEntry<?, ?>) o;
            return hash == other.hash && key.equals(other.key);
        }

        /**
         * Returns true if this entry is in the stack, false otherwise.
         */
        public boolean inStack() {
            return (nextInStack != null);
        }

        /**
         * Returns true if this entry is in the queue, false otherwise.
         */
        public boolean inQueue() {
            return (nextInQueue != null);
        }

        /**
         * Records a cache hit.
         */
        public void hit(Set<HashEntry<K, V>> evicted) {
            switch (state) {
                case LIR_RESIDENT:
                    hotHit(evicted);
                    break;
                case HIR_RESIDENT:
                    coldHit(evicted);
                    break;
                case HIR_NONRESIDENT:
                    throw new IllegalStateException("Can't hit a non-resident entry!");
                default:
                    throw new AssertionError("Hit with unknown status: " + state);
            }
        }

        /**
         * Records a cache hit on a hot block.
         */
        private void hotHit(Set<HashEntry<K, V>> evicted) {
            // See section 3.3 case 1:
            // "Upon accessing an LIR block X:
            // This access is guaranteed to be a hit in the cache."

            // "We move it to the top of stack S."
            boolean onBottom = (owner.stackBottom() == this);
            moveToStackTop();

            // "If the LIR block is originally located in the bottom of the stack,
            // we conduct a stack pruning."
            if (onBottom) {
                owner.pruneStack(evicted);
            }
        }

        /**
         * Records a cache hit on a cold block.
         */
        private void coldHit(Set<HashEntry<K, V>> evicted) {
            // See section 3.3 case 2:
            // "Upon accessing an HIR resident block X:
            // This is a hit in the cache."

            // "We move it to the top of stack S."
            boolean inStack = inStack();
            moveToStackTop();

            // "There are two cases for block X:"
            if (inStack) {
                // "(1) If X is in the stack S, we change its status to LIR."
                hot();

                // "This block is also removed from list Q."
                removeFromQueue();

                // "The LIR block in the bottom of S is moved to the end of list Q
                // with its status changed to HIR."
                owner.stackBottom().migrateToQueue();

                // "A stack pruning is then conducted."
                owner.pruneStack(evicted);
            }
            else {
                // "(2) If X is not in stack S, we leave its status in HIR and move
                // it to the end of list Q."
                moveToQueueEnd();
            }
        }

        /**
         * Records a cache miss. This is how new entries join the LIRS stack and
         * queue. This is called both when a new entry is first created, and when a
         * non-resident entry is re-computed.
         */
        private Set<HashEntry<K, V>> miss() {
            Set<HashEntry<K, V>> evicted = Collections.emptySet();
            if (owner.hotSize < owner.maximumHotSize) {
                warmupMiss();
            }
            else {
                evicted = new HashSet<HashEntry<K, V>>();
                fullMiss(evicted);
            }

            // now the missed item is in the cache
            owner.size++;
            return evicted;
        }

        /**
         * Records a miss when the hot entry set is not full.
         */
        private void warmupMiss() {
            // See section 3.3:
            // "When LIR block set is not full, all the referenced blocks are
            // given an LIR status until its size reaches L_lirs."
            hot();
            moveToStackTop();
        }

        /**
         * Records a miss when the hot entry set is full.
         */
        private void fullMiss(Set<HashEntry<K, V>> evicted) {
            // See section 3.3 case 3:
            // "Upon accessing an HIR non-resident block X:
            // This is a miss."

            // This condition is unspecified in the paper, but appears to be
            // necessary.
            if (owner.size >= owner.maximumSize) {
                // "We remove the HIR resident block at the front of list Q (it then
                // becomes a non-resident block), and replace it out of the cache."
                LIRSHashEntry<K, V> evictedNode = owner.queueFront();
                evicted.add(evictedNode);
            }

            // "Then we load the requested block X into the freed buffer and place
            // it on the top of stack S."
            boolean inStack = inStack();
            moveToStackTop();

            // "There are two cases for block X:"
            if (inStack) {
                // "(1) If X is in stack S, we change its status to LIR and move the
                // LIR block in the bottom of stack S to the end of list Q with its
                // status changed to HIR. A stack pruning is then conducted.
                hot();
                owner.stackBottom().migrateToQueue();
                owner.pruneStack(evicted);
            }
            else {
                // "(2) If X is not in stack S, we leave its status in HIR and place
                // it in the end of list Q."
                cold();
            }
        }

        /**
         * Marks this entry as hot.
         */
        private void hot() {
            if (state != Recency.LIR_RESIDENT) {
                owner.hotSize++;
            }
            state = Recency.LIR_RESIDENT;
        }

        /**
         * Marks this entry as cold.
         */
        private void cold() {
            if (state == Recency.LIR_RESIDENT) {
                owner.hotSize--;
            }
            state = Recency.HIR_RESIDENT;
            moveToQueueEnd();
        }

        /**
         * Marks this entry as non-resident.
         */
        @SuppressWarnings("fallthrough")
        private void nonResident() {
            switch (state) {
                case LIR_RESIDENT:
                    owner.hotSize--;
                    // fallthrough
                case HIR_RESIDENT:
                    owner.size--;
                    break;
            }
            state = Recency.HIR_NONRESIDENT;
        }

        /**
         * Returns true if this entry is resident in the cache, false otherwise.
         */
        public boolean isResident() {
            return (state != Recency.HIR_NONRESIDENT);
        }

        /**
         * Temporarily removes this entry from the stack, fixing up neighbor links.
         * This entry's links remain unchanged, meaning that {@link #inStack()} will
         * continue to return true. This should only be called if this node's links
         * will be subsequently changed.
         */
        private void tempRemoveFromStack() {
            if (inStack()) {
                previousInStack.nextInStack = nextInStack;
                nextInStack.previousInStack = previousInStack;
            }
        }

        /**
         * Removes this entry from the stack.
         */
        private void removeFromStack() {
            tempRemoveFromStack();
            previousInStack = null;
            nextInStack = null;
        }

        /**
         * Inserts this entry before the specified existing entry in the stack.
         */
        private void addToStackBefore(LIRSHashEntry<K, V> existingEntry) {
            previousInStack = existingEntry.previousInStack;
            nextInStack = existingEntry;
            previousInStack.nextInStack = this;
            nextInStack.previousInStack = this;
        }

        /**
         * Moves this entry to the top of the stack.
         */
        private void moveToStackTop() {
            tempRemoveFromStack();
            addToStackBefore(owner.header.nextInStack);
        }

        /**
         * Moves this entry to the bottom of the stack.
         */
        private void moveToStackBottom() {
            tempRemoveFromStack();
            addToStackBefore(owner.header);
        }

        /**
         * Temporarily removes this entry from the queue, fixing up neighbor links.
         * This entry's links remain unchanged. This should only be called if this
         * node's links will be subsequently changed.
         */
        private void tempRemoveFromQueue() {
            if (inQueue()) {
                previousInQueue.nextInQueue = nextInQueue;
                nextInQueue.previousInQueue = previousInQueue;
            }
        }

        /**
         * Removes this entry from the queue.
         */
        private void removeFromQueue() {
            tempRemoveFromQueue();
            previousInQueue = null;
            nextInQueue = null;
        }

        /**
         * Inserts this entry before the specified existing entry in the queue.
         */
        private void addToQueueBefore(LIRSHashEntry<K, V> existingEntry) {
            previousInQueue = existingEntry.previousInQueue;
            nextInQueue = existingEntry;
            previousInQueue.nextInQueue = this;
            nextInQueue.previousInQueue = this;
        }

        /**
         * Moves this entry to the end of the queue.
         */
        private void moveToQueueEnd() {
            tempRemoveFromQueue();
            addToQueueBefore(owner.header);
        }

        /**
         * Moves this entry from the stack to the queue, marking it cold
         * (as hot entries must remain in the stack). This should only be called
         * on resident entries, as non-resident entries should not be made resident.
         * The bottom entry on the queue is always hot due to stack pruning.
         */
        private void migrateToQueue() {
            removeFromStack();
            cold();
        }

        /**
         * Moves this entry from the queue to the stack, marking it hot (as cold
         * resident entries must remain in the queue).
         */
        private void migrateToStack() {
            removeFromQueue();
            if (!inStack()) {
                moveToStackBottom();
            }
            hot();
        }

        /**
         * Evicts this entry, removing it from the queue and setting its status to
         * cold non-resident. If the entry is already absent from the stack, it is
         * removed from the backing map; otherwise it remains in order for its
         * recency to be maintained.
         */
        private void evict() {
            removeFromQueue();
            removeFromStack();
            nonResident();
            owner = null;
        }

        /**
         * Removes this entry from the cache. This operation is not specified in
         * the paper, which does not account for forced eviction.
         */
        private V remove() {
            boolean wasHot = (state == Recency.LIR_RESIDENT);
            V result = value;
            LIRSHashEntry<K, V> end = owner != null ? owner.queueEnd() : null;
            evict();

            // attempt to maintain a constant number of hot entries
            if (wasHot) {
                if (end != null) {
                    end.migrateToStack();
                }
            }

            return result;
        }
    }

    static final class LIRS<K, V> implements EvictionPolicy<K, V> {

        /**
         * The percentage of the cache which is dedicated to hot blocks.
         * See section 5.1
         */
        private static final float L_LIRS = 0.95f;

        /**
         * The owning segment
         */
        private final Segment<K, V> segment;

        /**
         * The accessQueue for reducing lock contention
         * See "BP-Wrapper: a system framework making any replacement algorithms
         * (almost) lock contention free"
         * <p/>
         * http://www.cse.ohio-state.edu/hpcs/WWW/HTML/publications/abs09-1.html
         */
        private final ConcurrentLinkedQueue<LIRSHashEntry<K, V>> accessQueue;

        /**
         * The maxBatchQueueSize
         * <p/>
         * See "BP-Wrapper: a system framework making any replacement algorithms (almost) lock
         * contention free"
         */
        private final int maxBatchQueueSize;

        /**
         * The number of LIRS entries in a segment
         */
        private int size;

        private final float batchThresholdFactor;

        /**
         * This header encompasses two data structures:
         * <p/>
         * <ul>
         * <li>The LIRS stack, S, which is maintains recency information. All hot
         * entries are on the stack. All cold and non-resident entries which are more
         * recent than the least recent hot entry are also stored in the stack (the
         * stack is always pruned such that the last entry is hot, and all entries
         * accessed more recently than the last hot entry are present in the stack).
         * The stack is ordered by recency, with its most recently accessed entry
         * at the top, and its least recently accessed entry at the bottom.</li>
         * <p/>
         * <li>The LIRS queue, Q, which enqueues all cold entries for eviction. Cold
         * entries (by definition in the queue) may be absent from the stack (due to
         * pruning of the stack). Cold entries are added to the end of the queue
         * and entries are evicted from the front of the queue.</li>
         * </ul>
         */
        private final LIRSHashEntry<K, V> header = new LIRSHashEntry<K, V>(null, null, 0, null, null);

        /**
         * The maximum number of hot entries (L_lirs in the paper).
         */
        private final int maximumHotSize;

        /**
         * The maximum number of resident entries (L in the paper).
         */
        private final int maximumSize;

        /**
         * The actual number of hot entries.
         */
        private int hotSize;

        public LIRS(Segment<K, V> s, int capacity, int maxBatchSize, float batchThresholdFactor) {
            this.segment = s;
            this.maximumSize = capacity;
            this.maximumHotSize = calculateLIRSize(capacity);
            this.maxBatchQueueSize = maxBatchSize > MAX_BATCH_SIZE ? MAX_BATCH_SIZE : maxBatchSize;
            this.batchThresholdFactor = batchThresholdFactor;
            this.accessQueue = new ConcurrentLinkedQueue<LIRSHashEntry<K, V>>();
        }

        private static int calculateLIRSize(int maximumSize) {
            int result = (int) (L_LIRS * maximumSize);
            return (result == maximumSize) ? maximumSize - 1 : result;
        }

        @Override
        public Set<HashEntry<K, V>> execute() {
            Set<HashEntry<K, V>> evicted = new HashSet<HashEntry<K, V>>();
            try {
                for (LIRSHashEntry<K, V> e : accessQueue) {
                    if (e.isResident()) {
                        e.hit(evicted);
                    }
                }
                removeFromSegment(evicted);
            }
            finally {
                accessQueue.clear();
            }
            return evicted;
        }

        /**
         * Prunes HIR blocks in the bottom of the stack until an HOT block sits in
         * the stack bottom. If pruned blocks were resident, then they
         * remain in the queue; otherwise they are no longer referenced, and are thus
         * removed from the backing map.
         */
        private void pruneStack(Set<HashEntry<K, V>> evicted) {
            // See section 3.3:
            // "We define an operation called "stack pruning" on the LIRS
            // stack S, which removes the HIR blocks in the bottom of
            // the stack until an LIR block sits in the stack bottom. This
            // operation serves for two purposes: (1) We ensure the block in
            // the bottom of the stack always belongs to the LIR block set.
            // (2) After the LIR block in the bottom is removed, those HIR
            // blocks contiguously located above it will not have chances to
            // change their status from HIR to LIR, because their recencies
            // are larger than the new maximum recency of LIR blocks."
            LIRSHashEntry<K, V> bottom = stackBottom();
            while (bottom != null && bottom.state != Recency.LIR_RESIDENT) {
                bottom.removeFromStack();
                if (bottom.state == Recency.HIR_NONRESIDENT) {
                    evicted.add(bottom);
                }
                bottom = stackBottom();
            }
        }

        @Override
        public Set<HashEntry<K, V>> onEntryMiss(HashEntry<K, V> en) {
            LIRSHashEntry<K, V> e = (LIRSHashEntry<K, V>) en;
            Set<HashEntry<K, V>> evicted = e.miss();
            removeFromSegment(evicted);
            return evicted;
        }

        private void removeFromSegment(Set<HashEntry<K, V>> evicted) {
            for (HashEntry<K, V> e : evicted) {
                ((LIRSHashEntry<K, V>) e).evict();
                segment.evictionListener.onEntryChosenForEviction(e.value);
                segment.remove(e.key, e.hash, null);
            }
        }

        /*
         * Invoked without holding a lock on Segment
         */
        @Override
        public boolean onEntryHit(HashEntry<K, V> e) {
            accessQueue.add((LIRSHashEntry<K, V>) e);
            return accessQueue.size() >= maxBatchQueueSize * batchThresholdFactor;
        }

        /*
         * Invoked without holding a lock on Segment
         */
        @Override
        public boolean thresholdExpired() {
            return accessQueue.size() >= maxBatchQueueSize;
        }

        @Override
        public void onEntryRemove(HashEntry<K, V> e) {

            ((LIRSHashEntry<K, V>) e).remove();
            // we could have multiple instances of e in accessQueue; remove them all
            while (accessQueue.remove(e)) {
            }
        }

        @Override
        public void clear() {
            accessQueue.clear();
        }

        @Override
        public Eviction strategy() {
            return Eviction.LIRS;
        }

        /**
         * Returns the entry at the bottom of the stack.
         */
        private LIRSHashEntry<K, V> stackBottom() {
            LIRSHashEntry<K, V> bottom = header.previousInStack;
            return (bottom == header) ? null : bottom;
        }

        /**
         * Returns the entry at the front of the queue.
         */
        private LIRSHashEntry<K, V> queueFront() {
            LIRSHashEntry<K, V> front = header.nextInQueue;
            return (front == header) ? null : front;
        }

        /**
         * Returns the entry at the end of the queue.
         */
        private LIRSHashEntry<K, V> queueEnd() {
            LIRSHashEntry<K, V> end = header.previousInQueue;
            return (end == header) ? null : end;
        }

        @Override
        public HashEntry<K, V> createNewEntry(K key, int hash, HashEntry<K, V> next, V value) {
            return new LIRSHashEntry<K, V>(this, key, hash, next, value);
        }
    }

    /**
     * Segments are specialized versions of hash tables.  This
     * subclasses from ReentrantLock opportunistically, just to
     * simplify some locking and avoid separate construction.
     */
    static final class Segment<K, V> extends ReentrantLock {
        /*
         * Segments maintain a table of entry lists that are ALWAYS
         * kept in a consistent state, so can be read without locking.
         * Next fields of nodes are immutable (final). All list
         * additions are performed at the front of each bin. This
         * makes it easy to check changes, and also fast to traverse.
         * When nodes would otherwise be changed, new nodes are
         * created to replace them. This works well for hash tables
         * since the bin lists tend to be short. (The average length
         * is less than two for the default load factor threshold.)
         *
         * Read operations can thus proceed without locking, but rely
         * on selected uses of volatiles to ensure that completed
         * write operations performed by other threads are
         * noticed. For most purposes, the "count" field, tracking the
         * number of elements, serves as that volatile variable
         * ensuring visibility. This is convenient because this field
         * needs to be read in many read operations anyway:
         *
         * - All (unsynchronized) read operations must first read the
         * "count" field, and should not look at table entries if
         * it is 0.
         *
         * - All (synchronized) write operations should write to
         * the "count" field after structurally changing any bin.
         * The operations must not take any action that could even
         * momentarily cause a concurrent read operation to see
         * inconsistent data. This is made easier by the nature of
         * the read operations in Map. For example, no operation
         * can reveal that the table has grown but the threshold
         * has not yet been updated, so there are no atomicity
         * requirements for this with respect to reads.
         *
         * As a guide, all critical volatile reads and writes to the
         * count field are marked in code comments.
         */

        private static final long serialVersionUID = 2249069246763182397L;

        /**
         * The number of elements in this segment's region.
         */
        transient volatile int count;

        /**
         * Number of updates that alter the size of the table. This is
         * used during bulk-read methods to make sure they see a
         * consistent snapshot: If modCounts change during a traversal
         * of segments computing size or checking containsValue, then
         * we might have an inconsistent view of state so (usually)
         * must retry.
         */
        transient int modCount;

        /**
         * The table is rehashed when its size exceeds this threshold.
         * (The value of this field is always <tt>(int)(capacity *
         * loadFactor)</tt>.)
         */
        transient int threshold;

        /**
         * The per-segment table.
         */
        transient volatile HashEntry<K, V>[] table;

        /**
         * The load factor for the hash table.  Even though this value
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         *
         * @serial
         */
        final float loadFactor;

        final int evictCap;

        transient final EvictionPolicy<K, V> eviction;

        transient final EvictionListener<K, V> evictionListener;

        Segment(int cap, int evictCap, float lf, Eviction es, EvictionListener<K, V> listener) {
            loadFactor = lf;
            this.evictCap = evictCap;
            eviction = es.make(this, evictCap, lf);
            evictionListener = listener;
            setTable(HashEntry.<K, V> newArray(cap));
        }

        @SuppressWarnings("unchecked")
        static <K, V> Segment<K, V>[] newArray(int i) {
            return new Segment[i];
        }

        EvictionListener<K, V> getEvictionListener() {
            return evictionListener;
        }

        /**
         * Sets table to new HashEntry array.
         * Call only while holding lock or in constructor.
         */
        void setTable(HashEntry<K, V>[] newTable) {
            threshold = (int) (newTable.length * loadFactor);
            table = newTable;
        }

        /**
         * Returns properly casted first entry of bin for given hash.
         */
        HashEntry<K, V> getFirst(int hash) {
            HashEntry<K, V>[] tab = table;
            return tab[hash & tab.length - 1];
        }

        /**
         * Reads value field of an entry under lock. Called if value
         * field ever appears to be null. This is possible only if a
         * compiler happens to reorder a HashEntry initialization with
         * its table assignment, which is legal under memory model
         * but is not known to ever occur.
         */
        V readValueUnderLock(HashEntry<K, V> e) {
            lock();
            try {
                return e.value;
            }
            finally {
                unlock();
            }
        }

        /* Specialized implementations of map methods */

        V get(Object key, int hash) {
            int c = count;
            if (c != 0) { // read-volatile
                V result = null;
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        V v = e.value;
                        if (v != null) {
                            result = v;
                            break;
                        }
                        else {
                            result = readValueUnderLock(e); // recheck
                            break;
                        }
                    }
                    e = e.next;
                }
                // a hit
                if (result != null) {
                    if (eviction.onEntryHit(e)) {
                        Set<HashEntry<K, V>> evicted = attemptEviction(false);
                        notifyEvictionListener(evicted);
                    }
                }
                return result;
            }
            return null;
        }

        boolean containsKey(Object key, int hash) {
            if (count != 0) { // read-volatile
                HashEntry<K, V> e = getFirst(hash);
                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        return true;
                    }
                    e = e.next;
                }
            }
            return false;
        }

        boolean containsValue(Object value) {
            if (count != 0) { // read-volatile
                HashEntry<K, V>[] tab = table;
                int len = tab.length;
                for (int i = 0; i < len; i++) {
                    for (HashEntry<K, V> e = tab[i]; e != null; e = e.next) {
                        V v = e.value;
                        if (v == null) {
                            v = readValueUnderLock(e);
                        }
                        if (value.equals(v)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        boolean replace(K key, int hash, V oldValue, V newValue) {
            lock();
            Set<HashEntry<K, V>> evicted = null;
            try {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null && (e.hash != hash || !key.equals(e.key))) {
                    e = e.next;
                }

                boolean replaced = false;
                if (e != null && oldValue.equals(e.value)) {
                    replaced = true;
                    e.value = newValue;
                    if (eviction.onEntryHit(e)) {
                        evicted = attemptEviction(true);
                    }
                }
                return replaced;
            }
            finally {
                unlock();
                notifyEvictionListener(evicted);
            }
        }

        V replace(K key, int hash, V newValue) {
            lock();
            Set<HashEntry<K, V>> evicted = null;
            try {
                HashEntry<K, V> e = getFirst(hash);
                while (e != null && (e.hash != hash || !key.equals(e.key))) {
                    e = e.next;
                }

                V oldValue = null;
                if (e != null) {
                    oldValue = e.value;
                    e.value = newValue;
                    if (eviction.onEntryHit(e)) {
                        evicted = attemptEviction(true);
                    }
                }
                return oldValue;
            }
            finally {
                unlock();
                notifyEvictionListener(evicted);
            }
        }

        V put(K key, int hash, V value, boolean onlyIfAbsent) {
            lock();
            Set<HashEntry<K, V>> evicted = null;
            try {
                int c = count;
                if (c++ > threshold && eviction.strategy() == Eviction.NONE) {
                    rehash();
                }
                HashEntry<K, V>[] tab = table;
                int index = hash & tab.length - 1;
                HashEntry<K, V> first = tab[index];
                HashEntry<K, V> e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key))) {
                    e = e.next;
                }

                V oldValue;
                if (e != null) {
                    oldValue = e.value;
                    if (!onlyIfAbsent) {
                        e.value = value;
                        eviction.onEntryHit(e);
                    }
                }
                else {
                    oldValue = null;
                    ++modCount;
                    count = c; // write-volatile
                    if (eviction.strategy() != Eviction.NONE) {
                        if (c > evictCap) {
                            // remove entries;lower count
                            evicted = eviction.execute();
                            // re-read first
                            first = tab[index];
                        }
                        // add a new entry
                        tab[index] = eviction.createNewEntry(key, hash, first, value);
                        // notify a miss
                        Set<HashEntry<K, V>> newlyEvicted = eviction.onEntryMiss(tab[index]);
                        if (!newlyEvicted.isEmpty()) {
                            if (evicted != null) {
                                evicted.addAll(newlyEvicted);
                            }
                            else {
                                evicted = newlyEvicted;
                            }
                        }
                    }
                    else {
                        tab[index] = eviction.createNewEntry(key, hash, first, value);
                    }
                }
                return oldValue;
            }
            finally {
                unlock();
                notifyEvictionListener(evicted);
            }
        }

        void rehash() {
            HashEntry<K, V>[] oldTable = table;
            int oldCapacity = oldTable.length;
            if (oldCapacity >= MAXIMUM_CAPACITY) {
                return;
            }

            /*
             * Reclassify nodes in each list to new Map. Because we are
             * using power-of-two expansion, the elements from each bin
             * must either stay at same index, or move with a power of two
             * offset. We eliminate unnecessary node creation by catching
             * cases where old nodes can be reused because their next
             * fields won't change. Statistically, at the default
             * threshold, only about one-sixth of them need cloning when
             * a table doubles. The nodes they replace will be garbage
             * collectable as soon as they are no longer referenced by any
             * reader thread that may be in the midst of traversing table
             * right now.
             */

            HashEntry<K, V>[] newTable = HashEntry.newArray(oldCapacity << 1);
            threshold = (int) (newTable.length * loadFactor);
            int sizeMask = newTable.length - 1;
            for (int i = 0; i < oldCapacity; i++) {
                // We need to guarantee that any existing reads of old Map can
                // proceed. So we cannot yet null out each bin.
                HashEntry<K, V> e = oldTable[i];

                if (e != null) {
                    HashEntry<K, V> next = e.next;
                    int idx = e.hash & sizeMask;

                    // Single node on list
                    if (next == null) {
                        newTable[idx] = e;
                    }
                    else {
                        // Reuse trailing consecutive sequence at same slot
                        HashEntry<K, V> lastRun = e;
                        int lastIdx = idx;
                        for (HashEntry<K, V> last = next; last != null; last = last.next) {
                            int k = last.hash & sizeMask;
                            if (k != lastIdx) {
                                lastIdx = k;
                                lastRun = last;
                            }
                        }
                        newTable[lastIdx] = lastRun;

                        // Clone all remaining nodes
                        for (HashEntry<K, V> p = e; p != lastRun; p = p.next) {
                            int k = p.hash & sizeMask;
                            HashEntry<K, V> n = newTable[k];
                            newTable[k] = eviction.createNewEntry(p.key, p.hash, n, p.value);
                        }
                    }
                }
            }
            table = newTable;
        }

        /**
         * Remove; match on key only if value null, else match both.
         */
        V remove(Object key, int hash, Object value) {
            lock();
            try {
                int c = count - 1;
                HashEntry<K, V>[] tab = table;
                int index = hash & tab.length - 1;
                HashEntry<K, V> first = tab[index];
                HashEntry<K, V> e = first;
                while (e != null && (e.hash != hash || !key.equals(e.key))) {
                    e = e.next;
                }

                V oldValue = null;
                if (e != null) {
                    V v = e.value;
                    if (value == null || value.equals(v)) {
                        oldValue = v;
                        // All entries following removed node can stay
                        // in list, but all preceding ones need to be
                        // cloned.
                        ++modCount;

                        // e was removed
                        eviction.onEntryRemove(e);

                        HashEntry<K, V> newFirst = e.next;
                        for (HashEntry<K, V> p = first; p != e; p = p.next) {
                            // TODO A remove operation makes the map behave like all the other keys in the bucket were just added???
                            // allow p to be GC-ed
                            eviction.onEntryRemove(p);
                            newFirst = eviction.createNewEntry(p.key, p.hash, newFirst, p.value);
                            // and notify eviction algorithm about new hash entries
                            eviction.onEntryMiss(newFirst);
                        }

                        tab[index] = newFirst;
                        count = c; // write-volatile
                    }
                }
                return oldValue;
            }
            finally {
                unlock();
            }
        }

        void clear() {
            if (count != 0) {
                lock();
                try {
                    HashEntry<K, V>[] tab = table;
                    for (int i = 0; i < tab.length; i++) {
                        tab[i] = null;
                    }
                    ++modCount;
                    eviction.clear();
                    count = 0; // write-volatile
                }
                finally {
                    unlock();
                }
            }
        }

        private Set<HashEntry<K, V>> attemptEviction(boolean lockedAlready) {
            Set<HashEntry<K, V>> evicted = null;
            boolean obtainedLock = lockedAlready || tryLock();
            if (!obtainedLock && eviction.thresholdExpired()) {
                lock();
                obtainedLock = true;
            }
            if (obtainedLock) {
                try {
                    if (eviction.thresholdExpired()) {
                        evicted = eviction.execute();
                    }
                }
                finally {
                    if (!lockedAlready) {
                        unlock();
                    }
                }
            }
            return evicted;
        }

        private void notifyEvictionListener(Set<HashEntry<K, V>> evicted) {
            // piggyback listener invocation on callers thread outside lock
            if (evicted != null) {
                Map<K, V> evictedCopy;
                if (evicted.size() == 1) {
                    HashEntry<K, V> evictedEntry = evicted.iterator().next();
                    evictedCopy = singletonMap(evictedEntry.key, evictedEntry.value);
                }
                else {
                    evictedCopy = new HashMap<K, V>(evicted.size());
                    for (HashEntry<K, V> he : evicted) {
                        evictedCopy.put(he.key, he.value);
                    }
                    evictedCopy = unmodifiableMap(evictedCopy);
                }
                evictionListener.onEntryEviction(evictedCopy);
            }
        }
    }

    /* ---------------- Public operations -------------- */

    /**
     * Creates a new, empty map with the specified maximum capacity, load factor and concurrency
     * level.
     *
     * @param capacity is the upper bound capacity for the number of elements in this map
     * @param concurrencyLevel the estimated number of concurrently updating threads. The implementation performs
     * internal sizing to try to accommodate this many threads.
     * @param evictionStrategy the algorithm used to evict elements from this map
     * @param evictionListener the evicton listener callback to be notified about evicted elements
     *
     * @throws IllegalArgumentException if the initial capacity is negative or the load factor or concurrencyLevel are
     * nonpositive.
     */
    public BoundedConcurrentHashMap(
                                    int capacity, int concurrencyLevel,
                                    Eviction evictionStrategy, EvictionListener<K, V> evictionListener) {
        if (capacity < 0 || concurrencyLevel <= 0) {
            throw new IllegalArgumentException();
        }

        concurrencyLevel = Math.min(capacity / 2, concurrencyLevel); // concurrencyLevel cannot be > capacity/2
        concurrencyLevel = Math.max(concurrencyLevel, 1); // concurrencyLevel cannot be less than 1

        // minimum two elements per segment
        if (capacity < concurrencyLevel * 2 && capacity != 1) {
            throw new IllegalArgumentException("Maximum capacity has to be at least twice the concurrencyLevel");
        }

        if (evictionStrategy == null || evictionListener == null) {
            throw new IllegalArgumentException();
        }

        if (concurrencyLevel > MAX_SEGMENTS) {
            concurrencyLevel = MAX_SEGMENTS;
        }

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = Segment.newArray(ssize);

        if (capacity > MAXIMUM_CAPACITY) {
            capacity = MAXIMUM_CAPACITY;
        }
        int c = capacity / ssize;
        int cap = 1;
        while (cap < c) {
            cap <<= 1;
        }

        for (int i = 0; i < this.segments.length; ++i) {
            this.segments[i] = new Segment<K, V>(cap, c, DEFAULT_LOAD_FACTOR, evictionStrategy, evictionListener);
        }
    }

    /**
     * Creates a new, empty map with the specified maximum capacity, load factor, concurrency
     * level and LRU eviction policy.
     *
     * @param capacity is the upper bound capacity for the number of elements in this map
     * @param concurrencyLevel the estimated number of concurrently updating threads. The implementation performs
     * internal sizing to try to accommodate this many threads.
     *
     * @throws IllegalArgumentException if the initial capacity is negative or the load factor or concurrencyLevel are
     * nonpositive.
     */
    public BoundedConcurrentHashMap(int capacity, int concurrencyLevel) {
        this(capacity, concurrencyLevel, Eviction.LRU);
    }

    /**
     * Creates a new, empty map with the specified maximum capacity, load factor, concurrency
     * level and eviction strategy.
     *
     * @param capacity is the upper bound capacity for the number of elements in this map
     * @param concurrencyLevel the estimated number of concurrently updating threads. The implementation performs
     * internal sizing to try to accommodate this many threads.
     * @param evictionStrategy the algorithm used to evict elements from this map
     *
     * @throws IllegalArgumentException if the initial capacity is negative or the load factor or concurrencyLevel are
     * nonpositive.
     */
    public BoundedConcurrentHashMap(int capacity, int concurrencyLevel, Eviction evictionStrategy) {
        this(capacity, concurrencyLevel, evictionStrategy, new NullEvictionListener<K, V>());
    }

    /**
     * Creates a new, empty map with the specified maximum capacity, default concurrency
     * level and LRU eviction policy.
     *
     * @param capacity is the upper bound capacity for the number of elements in this map
     *
     * @throws IllegalArgumentException if the initial capacity of
     * elements is negative or the load factor is nonpositive
     * @since 1.6
     */
    public BoundedConcurrentHashMap(int capacity) {
        this(capacity, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Creates a new, empty map with the default maximum capacity
     */
    public BoundedConcurrentHashMap() {
        this(DEFAULT_MAXIMUM_CAPACITY, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    @Override
    public boolean isEmpty() {
        final Segment<K, V>[] segments = this.segments;
        /*
         * We keep track of per-segment modCounts to avoid ABA
         * problems in which an element in one segment was added and
         * in another removed during traversal, in which case the
         * table was never actually empty at any point. Note the
         * similar use of modCounts in the size() and containsValue()
         * methods, which are the only other methods also susceptible
         * to ABA problems.
         */
        int[] mc = new int[segments.length];
        int mcsum = 0;
        for (int i = 0; i < segments.length; ++i) {
            if (segments[i].count != 0) {
                return false;
            }
            else {
                mcsum += mc[i] = segments[i].modCount;
            }
        }
        // If mcsum happens to be zero, then we know we got a snapshot
        // before any modifications at all were made. This is
        // probably common enough to bother tracking.
        if (mcsum != 0) {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].count != 0 || mc[i] != segments[i].modCount) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the number of key-value mappings in this map.  If the
     * map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        final Segment<K, V>[] segments = this.segments;
        long sum = 0;
        long check = 0;
        int[] mc = new int[segments.length];
        // Try a few times to get accurate count. On failure due to
        // continuous async changes in table, resort to locking.
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            check = 0;
            sum = 0;
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                sum += segments[i].count;
                mcsum += mc[i] = segments[i].modCount;
            }
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    check += segments[i].count;
                    if (mc[i] != segments[i].modCount) {
                        check = -1; // force retry
                        break;
                    }
                }
            }
            if (check == sum) {
                break;
            }
        }
        if (check != sum) { // Resort to locking all segments
            sum = 0;
            for (int i = 0; i < segments.length; ++i) {
                segments[i].lock();
            }
            try {
                for (int i = 0; i < segments.length; ++i) {
                    sum += segments[i].count;
                }
            }
            finally {
                for (int i = 0; i < segments.length; ++i) {
                    segments[i].unlock();
                }
            }
        }
        if (sum > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        else {
            return (int) sum;
        }
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p/>
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key.equals(k)},
     * then this method returns {@code v}; otherwise it returns
     * {@code null}.  (There can be at most one such mapping.)
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public V get(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).get(key, hash);
    }

    /**
     * Tests if the specified object is a key in this table.
     *
     * @param key possible key
     *
     * @return <tt>true</tt> if and only if the specified object
     * is a key in this table, as determined by the
     * <tt>equals</tt> method; <tt>false</tt> otherwise.
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean containsKey(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).containsKey(key, hash);
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value. Note: This method requires a full internal
     * traversal of the hash table, and so is much slower than
     * method <tt>containsKey</tt>.
     *
     * @param value value whose presence in this map is to be tested
     *
     * @return <tt>true</tt> if this map maps one or more keys to the
     * specified value
     *
     * @throws NullPointerException if the specified value is null
     */
    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            throw new NullPointerException();
        }

        // See explanation of modCount use above

        final Segment<K, V>[] segments = this.segments;
        int[] mc = new int[segments.length];

        // Try a few times without locking
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            int mcsum = 0;
            for (int i = 0; i < segments.length; ++i) {
                @SuppressWarnings("unused")
                int c = segments[i].count; // read-volatile
                mcsum += mc[i] = segments[i].modCount;
                if (segments[i].containsValue(value)) {
                    return true;
                }
            }
            boolean cleanSweep = true;
            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    @SuppressWarnings("unused")
                    int c = segments[i].count; // read-volatile
                    if (mc[i] != segments[i].modCount) {
                        cleanSweep = false;
                        break;
                    }
                }
            }
            if (cleanSweep) {
                return false;
            }
        }
        // Resort to locking all segments
        for (int i = 0; i < segments.length; ++i) {
            segments[i].lock();
        }
        boolean found = false;
        try {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].containsValue(value)) {
                    found = true;
                    break;
                }
            }
        }
        finally {
            for (int i = 0; i < segments.length; ++i) {
                segments[i].unlock();
            }
        }
        return found;
    }

    /**
     * Legacy method testing if some key maps into the specified value
     * in this table.  This method is identical in functionality to
     * {@link #containsValue}, and exists solely to ensure
     * full compatibility with class {@link java.util.Hashtable},
     * which supported this method prior to introduction of the
     * Java Collections framework.
     *
     * @param value a value to search for
     *
     * @return <tt>true</tt> if and only if some key maps to the
     * <tt>value</tt> argument in this table as
     * determined by the <tt>equals</tt> method;
     * <tt>false</tt> otherwise
     *
     * @throws NullPointerException if the specified value is null
     */
    public boolean contains(Object value) {
        return containsValue(value);
    }

    /**
     * Maps the specified key to the specified value in this table.
     * Neither the key nor the value can be null.
     * <p/>
     * <p> The value can be retrieved by calling the <tt>get</tt> method
     * with a key that is equal to the original key.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     *
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>
     *
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public V put(K key, V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, value, false);
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or <tt>null</tt> if there was no mapping for the key
     *
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public V putIfAbsent(K key, V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, hash, value, true);
    }

    /**
     * Copies all of the mappings from the specified map to this one.
     * These mappings replace any mappings that this map had for any of the
     * keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @param key the key that needs to be removed
     *
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public V remove(Object key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).remove(key, hash, null);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean remove(Object key, Object value) {
        int hash = hash(key.hashCode());
        if (value == null) {
            return false;
        }
        return segmentFor(hash).remove(key, hash, value) != null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if any of the arguments are null
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        int hash = hash(key.hashCode());
        return segmentFor(hash).replace(key, hash, oldValue, newValue);
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or <tt>null</tt> if there was no mapping for the key
     *
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public V replace(K key, V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        int hash = hash(key.hashCode());
        return segmentFor(hash).replace(key, hash, value);
    }

    /**
     * Removes all of the mappings from this map.
     */
    @Override
    public void clear() {
        for (int i = 0; i < segments.length; ++i) {
            segments[i].clear();
        }
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from this map,
     * via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or
     * <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link java.util.ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     */
    @Override
    public Set<K> keySet() {
        Set<K> ks = keySet;
        return ks != null ? ks : (keySet = new KeySet());
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  The collection
     * supports element removal, which removes the corresponding
     * mapping from this map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt>, and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link java.util.ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     */
    @Override
    public Collection<V> values() {
        Collection<V> vs = values;
        return vs != null ? vs : (values = new Values());
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or
     * <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link java.util.ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        Set<Map.Entry<K, V>> es = entrySet;
        return es != null ? es : (entrySet = new EntrySet());
    }

    /**
     * Returns an enumeration of the keys in this table.
     *
     * @return an enumeration of the keys in this table
     *
     * @see #keySet()
     */
    public Enumeration<K> keys() {
        return new KeyIterator();
    }

    /**
     * Returns an enumeration of the values in this table.
     *
     * @return an enumeration of the values in this table
     *
     * @see #values()
     */
    public Enumeration<V> elements() {
        return new ValueIterator();
    }

    /* ---------------- Iterator Support -------------- */

    abstract class HashIterator {
        int nextSegmentIndex;

        int nextTableIndex;

        HashEntry<K, V>[] currentTable;

        HashEntry<K, V> nextEntry;

        HashEntry<K, V> lastReturned;

        HashIterator() {
            nextSegmentIndex = segments.length - 1;
            nextTableIndex = -1;
            advance();
        }

        public boolean hasMoreElements() {
            return hasNext();
        }

        final void advance() {
            if (nextEntry != null && (nextEntry = nextEntry.next) != null) {
                return;
            }

            while (nextTableIndex >= 0) {
                if ((nextEntry = currentTable[nextTableIndex--]) != null) {
                    return;
                }
            }

            while (nextSegmentIndex >= 0) {
                Segment<K, V> seg = segments[nextSegmentIndex--];
                if (seg.count != 0) {
                    currentTable = seg.table;
                    for (int j = currentTable.length - 1; j >= 0; --j) {
                        if ((nextEntry = currentTable[j]) != null) {
                            nextTableIndex = j - 1;
                            return;
                        }
                    }
                }
            }
        }

        public boolean hasNext() {
            return nextEntry != null;
        }

        HashEntry<K, V> nextEntry() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            BoundedConcurrentHashMap.this.remove(lastReturned.key);
            lastReturned = null;
        }
    }

    final class KeyIterator extends HashIterator implements Iterator<K>, Enumeration<K> {
        @Override
        public K next() {
            return super.nextEntry().key;
        }

        @Override
        public K nextElement() {
            return super.nextEntry().key;
        }
    }

    final class ValueIterator extends HashIterator implements Iterator<V>, Enumeration<V> {
        @Override
        public V next() {
            return super.nextEntry().value;
        }

        @Override
        public V nextElement() {
            return super.nextEntry().value;
        }
    }

    /**
     * Custom Entry class used by EntryIterator.next(), that relays
     * setValue changes to the underlying map.
     */
    final class WriteThroughEntry extends AbstractMap.SimpleEntry<K, V> {

        private static final long serialVersionUID = -7041346694785573824L;

        WriteThroughEntry(K k, V v) {
            super(k, v);
        }

        /**
         * Set our entry's value and write through to the map. The
         * value to return is somewhat arbitrary here. Since a
         * WriteThroughEntry does not necessarily track asynchronous
         * changes, the most recent "previous" value could be
         * different from what we return (or could even have been
         * removed in which case the put will re-establish). We do not
         * and cannot guarantee more.
         */
        @Override
        public V setValue(V value) {
            if (value == null) {
                throw new NullPointerException();
            }
            V v = super.setValue(value);
            BoundedConcurrentHashMap.this.put(getKey(), value);
            return v;
        }
    }

    final class EntryIterator extends HashIterator implements Iterator<Entry<K, V>> {
        @Override
        public Map.Entry<K, V> next() {
            HashEntry<K, V> e = super.nextEntry();
            return new WriteThroughEntry(e.key, e.value);
        }
    }

    final class KeySet extends AbstractSet<K> {
        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        @Override
        public int size() {
            return BoundedConcurrentHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return BoundedConcurrentHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return BoundedConcurrentHashMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return BoundedConcurrentHashMap.this.remove(o) != null;
        }

        @Override
        public void clear() {
            BoundedConcurrentHashMap.this.clear();
        }
    }

    final class Values extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        @Override
        public int size() {
            return BoundedConcurrentHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return BoundedConcurrentHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return BoundedConcurrentHashMap.this.containsValue(o);
        }

        @Override
        public void clear() {
            BoundedConcurrentHashMap.this.clear();
        }
    }

    final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            V v = BoundedConcurrentHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            return BoundedConcurrentHashMap.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public int size() {
            return BoundedConcurrentHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return BoundedConcurrentHashMap.this.isEmpty();
        }

        @Override
        public void clear() {
            BoundedConcurrentHashMap.this.clear();
        }
    }

    /* ---------------- Serialization Support -------------- */

    /**
     * Save the state of the <tt>ConcurrentHashMap</tt> instance to a
     * stream (i.e., serialize it).
     *
     * @param s the stream
     *
     * @serialData the key (Object) and value (Object)
     * for each key-value mapping, followed by a null pair.
     * The key-value mappings are emitted in no particular order.
     */
    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();

        for (int k = 0; k < segments.length; ++k) {
            Segment<K, V> seg = segments[k];
            seg.lock();
            try {
                HashEntry<K, V>[] tab = seg.table;
                for (int i = 0; i < tab.length; ++i) {
                    for (HashEntry<K, V> e = tab[i]; e != null; e = e.next) {
                        s.writeObject(e.key);
                        s.writeObject(e.value);
                    }
                }
            }
            finally {
                seg.unlock();
            }
        }
        s.writeObject(null);
        s.writeObject(null);
    }

    /**
     * Reconstitute the <tt>ConcurrentHashMap</tt> instance from a
     * stream (i.e., deserialize it).
     *
     * @param s the stream
     */
    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream s) throws IOException,
            ClassNotFoundException {
        s.defaultReadObject();

        // Initialize each segment to be minimally sized, and let grow.
        for (int i = 0; i < segments.length; ++i) {
            segments[i].setTable(new HashEntry[1]);
        }

        // Read the keys and values, and put the mappings in the table
        for (;;) {
            K key = (K) s.readObject();
            V value = (V) s.readObject();
            if (key == null) {
                break;
            }
            put(key, value);
        }
    }
}
