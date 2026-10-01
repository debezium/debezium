/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ehcache.Cache;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.CacheProvider;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;
import io.debezium.connector.oracle.logminer.events.Xid;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Ehcache.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerTransactionCache extends AbstractLogMinerTransactionCache<EhcacheTransaction, EhcacheSlot> {

    private final Cache<Long, EhcacheTransaction> transactionCache;
    private final Cache<Long, LogMinerEvent> eventCache;
    private final EhcacheEvictionListener evictionListener;

    public EhcacheLogMinerTransactionCache(Cache<Long, EhcacheTransaction> transactionCache,
                                           Cache<Long, LogMinerEvent> eventCache,
                                           EhcacheEvictionListener evictionListener) {
        super(new EhcacheSegments());
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.evictionListener = evictionListener;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public EhcacheTransaction getTransaction(long xid) {
        final EhcacheSlot slot = segments.get(xid);
        return slot.key == null ? null : transactionCache.get(slot.key);
    }

    @Override
    public void addTransaction(EhcacheTransaction transaction) {
        final EhcacheSlot slot = segments.occupy(transaction.getXid());
        slot.key = Xid.key(transaction.getXid());
        slot.eventIds = new TreeSet<>();
        transactionCache.put(slot.key, transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
    }

    @Override
    public void removeTransaction(EhcacheTransaction transaction) {
        final EhcacheSlot slot = segments.vacate(transaction.getXid());
        if (slot.key != null) {
            transactionCache.remove(slot.key);
            slot.key = null;
        }
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<EhcacheTransaction>, R> consumer) {
        try (var stream = StreamSupport.stream(transactionCache.spliterator(), false)) {
            return consumer.apply(stream.map(Cache.Entry::getValue));
        }
    }

    @Override
    public void transactions(Consumer<Stream<EhcacheTransaction>> consumer) {
        try (var stream = StreamSupport.stream(transactionCache.spliterator(), false)) {
            consumer.accept(stream.map(Cache.Entry::getValue));
        }
    }

    @Override
    public void eventKeys(Consumer<LongStream> consumer) {
        try (var stream = StreamSupport.stream(eventCache.spliterator(), false)) {
            consumer.accept(stream.map(Cache.Entry::getKey).mapToLong(Xid::of));
        }
    }

    @Override
    public void forEachEvent(EhcacheTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
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
    public LogMinerEvent getTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        return eventCache.get(Xid.key(transaction.getEventId(eventKey)));
    }

    @Override
    public EhcacheTransaction getAndRemoveTransaction(long xid) {
        final EhcacheSlot slot = segments.vacate(xid);
        if (slot.key == null) {
            return null;
        }
        final EhcacheTransaction transaction = transactionCache.get(slot.key);
        if (transaction != null) {
            transactionCache.remove(slot.key);
        }
        slot.key = null;
        return transaction;
    }

    @Override
    public void addTransactionEvent(EhcacheTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(Xid.key(transaction.getEventId(eventKey)), event);
        checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);
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
    public void removeTransactionEvents(EhcacheTransaction transaction) {
        final EhcacheSlot slot = segments.get(transaction.getXid());
        if (slot.eventIds != null) {
            eventCache.removeAll(slot.eventIds
                    .stream()
                    .mapToLong(transaction::getEventId)
                    .mapToObj(Xid::key)
                    .collect(Collectors.toSet()));
        }
        slot.eventIds = null;
    }

    @Override
    public boolean containsTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        // Uses the highest event key ever assigned rather than checking for presence directly
        // since a partial rollback may have removed the event's entry from the cache.
        final var events = segments.get(transaction.getXid()).eventIds;
        return events != null && !events.isEmpty() && events.last() >= eventKey;
    }

    @Override
    public int getTransactionEventCount(EhcacheTransaction transaction) {
        final var events = segments.get(transaction.getXid()).eventIds;
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public int getTransactionEvents() {
        int sum = 0;
        for (EhcacheSlot slot : segments) {
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
    public void resetTransactionToStart(EhcacheTransaction transaction) {
        super.resetTransactionToStart(transaction);
        syncTransaction(transaction);
    }

    @Override
    public void syncTransaction(EhcacheTransaction transaction) {
        // todo:
        // Perhaps we can look at pulling number of events out of Transaction and let that
        // be managed in the cache's heap, in which case we can avoid this put.

        // Necessary to synchronize state
        transactionCache.put(Xid.key(transaction.getXid()), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the Ehcache persistence caches contained data on start-up
        for (Cache.Entry<Long, EhcacheTransaction> entry : transactionCache) {
            Long key = entry.getKey();
            EhcacheSlot slot = segments.occupy(Xid.of(key));
            slot.key = key;
            slot.eventIds = new TreeSet<>();
        }
        eventKeys(keyStream -> {
            keyStream.forEach(key -> {
                final EhcacheSlot slot = segments.get(key | 0x00000000ffffffffL);
                if (slot.eventIds != null) {
                    slot.eventIds.add((int) key);
                }
            });
        });
    }

    private void checkAndThrowIfEviction(String cacheName) {
        if (evictionListener.hasEvictionBeenSeen()) {
            throw new CacheCapacityExceededException(cacheName);
        }
    }
}
