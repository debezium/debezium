/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ehcache.Cache;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.CacheProvider;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Ehcache.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerTransactionCache extends AbstractLogMinerTransactionCache<EhcacheTransaction> {

    private final Cache<Integer, EhcacheTransaction> transactionCache;
    private final Cache<Long, LogMinerEvent> eventCache;
    private final EhcacheEvictionListener evictionListener;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<Integer, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();

    public EhcacheLogMinerTransactionCache(Cache<Integer, EhcacheTransaction> transactionCache,
                                           Cache<Long, LogMinerEvent> eventCache,
                                           EhcacheEvictionListener evictionListener) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.evictionListener = evictionListener;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public EhcacheTransaction getTransaction(String transactionId) {
        return checkSqn(transactionId, transactionCache.get(getUsnSlt(transactionId)));
    }

    @Override
    public void addTransaction(EhcacheTransaction transaction) {
        transactionCache.put(transaction.getUsnSlt(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
        eventIdsByTransactionId.put(transaction.getUsnSlt(), new TreeSet<>());
    }

    @Override
    public void removeTransaction(EhcacheTransaction transaction) {
        transactionCache.remove(transaction.getUsnSlt());
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return eventIdsByTransactionId.containsKey(getUsnSlt(transactionId));
    }

    @Override
    public boolean isEmpty() {
        return eventIdsByTransactionId.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return eventIdsByTransactionId.size();
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
    public void eventKeys(Consumer<Stream<Long>> consumer) {
        try (var stream = StreamSupport.stream(eventCache.spliterator(), false)) {
            consumer.accept(stream.map(Cache.Entry::getKey));
        }
    }

    @Override
    public void forEachEvent(EhcacheTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        final var events = eventIdsByTransactionId.get(transaction.getUsnSlt());
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
        return eventCache.get(transaction.getEventId(eventKey));
    }

    @Override
    public EhcacheTransaction getAndRemoveTransaction(String transactionId) {
        final EhcacheTransaction transaction = getTransaction(transactionId);
        if (transaction != null) {
            transactionCache.remove(getUsnSlt(transactionId));
        }
        return transaction;
    }

    @Override
    public void addTransactionEvent(EhcacheTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
        checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);
        final TreeSet<Integer> eventIds = eventIdsByTransactionId.get(transaction.getUsnSlt());
        eventIds.add(eventKey);

        if (event instanceof RollbackToSavepointEvent) {
            final Iterator<LogMinerEventEntry> reverseIterator = new LogMinerEventEntryIterator(
                    eventIds.descendingIterator(), id -> eventCache.get(transaction.getEventId(id)));
            final LogMinerEventEntryRange range = findRolledBackRange(transaction.getTransactionId(), reverseIterator);
            if (range != null) {
                final Iterator<Integer> forwardIterator = eventIds.subSet(range.start().eventId(), range.end().eventId()).iterator();
                while (forwardIterator.hasNext()) {
                    eventCache.remove(transaction.getEventId(forwardIterator.next()));
                    forwardIterator.remove();
                }
            }
        }
    }

    @Override
    public void removeTransactionEvents(EhcacheTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getUsnSlt());
        if (events != null) {
            eventCache.removeAll(events
                    .stream()
                    .map(transaction::getEventId)
                    .collect(Collectors.toSet()));
        }
        eventIdsByTransactionId.remove(transaction.getUsnSlt());
    }

    @Override
    public boolean containsTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        // Uses the highest event key ever assigned rather than checking for presence directly
        // since a partial rollback may have removed the event's entry from the cache.
        final var events = eventIdsByTransactionId.get(transaction.getUsnSlt());
        return events != null && !events.isEmpty() && events.last() >= eventKey;
    }

    @Override
    public int getTransactionEventCount(EhcacheTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getUsnSlt());
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public int getTransactionEvents() {
        return eventIdsByTransactionId.values().stream().mapToInt(Set::size).sum();
    }

    @Override
    public void clear() {
        transactionCache.clear();
        eventCache.clear();
        eventIdsByTransactionId.clear();
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
        transactionCache.put(transaction.getUsnSlt(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the Ehcache persistence caches contained data on start-up
        eventKeys(keyStream -> {
            keyStream.forEach(key -> {
                final int usnSlt = (int) (key >>> 32);
                final int eventId = (int) (long) key;
                if (transactionCache.containsKey(usnSlt)) {
                    eventIdsByTransactionId.computeIfAbsent(usnSlt, k -> new TreeSet<>()).add(eventId);
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
