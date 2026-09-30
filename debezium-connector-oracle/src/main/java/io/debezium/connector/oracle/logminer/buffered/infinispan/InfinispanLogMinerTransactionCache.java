/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.commons.api.BasicCache;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;
import io.debezium.connector.oracle.logminer.events.Xid;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Infinispan.
 *
 * @author Chris Cranford
 */
public class InfinispanLogMinerTransactionCache extends AbstractLogMinerTransactionCache<InfinispanTransaction> {

    private final BasicCache<Long, InfinispanTransaction> transactionCache;
    private final BasicCache<Long, LogMinerEvent> eventCache;
    private final InfinispanSegments segments = new InfinispanSegments();

    public InfinispanLogMinerTransactionCache(BasicCache<Long, InfinispanTransaction> transactionCache, BasicCache<Long, LogMinerEvent> eventCache) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public InfinispanTransaction getTransaction(long xid) {
        final InfinispanSlot slot = segments.get(xid);
        return slot.key == null ? null : transactionCache.get(slot.key);
    }

    @Override
    public void addTransaction(InfinispanTransaction transaction) {
        final InfinispanSlot slot = segments.occupy(transaction.getXid());
        slot.key = Xid.key(transaction.getXid());
        slot.eventIds = new TreeSet<>();
        transactionCache.put(slot.key, transaction);
    }

    @Override
    public void removeTransaction(InfinispanTransaction transaction) {
        final InfinispanSlot slot = segments.vacate(transaction.getXid());
        if (slot.key != null) {
            transactionCache.remove(slot.key);
            slot.key = null;
        }
    }

    @Override
    public boolean containsTransaction(long xid) {
        return segments.get(xid).occupied();
    }

    @Override
    public boolean isEmpty() {
        return segments.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return segments.size();
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<InfinispanTransaction>, R> consumer) {
        try (Stream<InfinispanTransaction> stream = transactionCache.values().stream()) {
            return consumer.apply(stream);
        }
    }

    @Override
    public void transactions(Consumer<Stream<InfinispanTransaction>> consumer) {
        try (Stream<InfinispanTransaction> stream = transactionCache.values().stream()) {
            consumer.accept(stream);
        }
    }

    @Override
    public void eventKeys(Consumer<LongStream> consumer) {
        try (Stream<Long> stream = eventCache.keySet().stream()) {
            consumer.accept(stream.mapToLong(Xid::of));
        }
    }

    @Override
    public void forEachEvent(InfinispanTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        final var events = segments.get(transaction.getXid()).eventIds;
        if (events != null) {
            try (var stream = events.stream()) {
                final Iterator<Integer> iterator = stream.iterator();
                while (iterator.hasNext()) {
                    final LogMinerEvent event = getTransactionEvent(transaction, iterator.next());
                    if (!predicate.test(event)) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public LogMinerEvent getTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        return eventCache.get(Xid.key(transaction.getEventId(eventKey)));
    }

    @Override
    public InfinispanTransaction getAndRemoveTransaction(long xid) {
        final InfinispanSlot slot = segments.vacate(xid);
        if (slot.key == null) {
            return null;
        }
        // Intentionally blocking
        final InfinispanTransaction transaction = transactionCache.remove(slot.key);
        slot.key = null;
        return transaction;
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(Xid.key(transaction.getEventId(eventKey)), event);
        final TreeSet<Integer> eventIds = segments.get(transaction.getXid()).eventIds;
        eventIds.add(eventKey);

        if (event instanceof RollbackToSavepointEvent) {
            final Iterator<LogMinerEventEntry> reverseIterator = new LogMinerEventEntryIterator(
                    eventIds.descendingIterator(), id -> eventCache.get(Xid.key(transaction.getEventId(id))));
            final LogMinerEventEntryRange range = findRolledBackRange(transaction.getXid(), reverseIterator);
            if (range != null) {
                final Iterator<Integer> forwardIterator = eventIds.subSet(range.start().eventId(), range.end().eventId()).iterator();
                while (forwardIterator.hasNext()) {
                    eventCache.remove(Xid.key(transaction.getEventId(forwardIterator.next())));
                    forwardIterator.remove();
                }
            }
        }
    }

    @Override
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        final InfinispanSlot slot = segments.get(transaction.getXid());
        if (slot.eventIds != null) {
            slot.eventIds.descendingSet().stream().mapToLong(transaction::getEventId).map(Xid::key).forEach(eventCache::remove);
        }
        slot.eventIds = null;
    }

    @Override
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        // Uses the highest event key ever assigned rather than checking for presence directly
        // since a partial rollback may have removed the event's entry from the cache.
        final var events = segments.get(transaction.getXid()).eventIds;
        return events != null && !events.isEmpty() && events.last() >= eventKey;
    }

    @Override
    public int getTransactionEventCount(InfinispanTransaction transaction) {
        final var events = segments.get(transaction.getXid()).eventIds;
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public int getTransactionEvents() {
        int sum = 0;
        for (InfinispanSlot slot : segments) {
            if (slot.eventIds != null) {
                sum += slot.eventIds.size();
            }
        }
        return sum;
    }

    @Override
    public void clear() {
        transactionCache.clear();
        eventCache.clear();
        segments.clear();
    }

    @Override
    public void resetTransactionToStart(InfinispanTransaction transaction) {
        super.resetTransactionToStart(transaction);
        syncTransaction(transaction);
    }

    @Override
    public void syncTransaction(InfinispanTransaction transaction) {
        // todo:
        // Perhaps we can look at pulling number of events out of Transaction and let that
        // be managed in the cache's heap, in which case we can avoid this put.

        // Necessary to synchronize state
        transactionCache.put(Xid.key(transaction.getXid()), transaction);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the Infinispan disk caches contained data on start-up
        try (Stream<Long> stream = transactionCache.keySet().stream()) {
            stream.forEach(key -> {
                InfinispanSlot slot = segments.occupy(Xid.of(key));
                slot.key = key;
                slot.eventIds = new TreeSet<>();
            });
        }
        eventKeys(keyStream -> {
            keyStream.forEach(key -> {
                final InfinispanSlot slot = segments.get(key | 0x00000000ffffffffL);
                if (slot.eventIds != null) {
                    slot.eventIds.add((int) key);
                }
            });
        });
    }
}
