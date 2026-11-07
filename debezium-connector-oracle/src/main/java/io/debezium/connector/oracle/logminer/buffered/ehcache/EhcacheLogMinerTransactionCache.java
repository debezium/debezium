/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
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

    private final Cache<String, EhcacheTransaction> transactionCache;
    private final Cache<String, LogMinerEvent> eventCache;
    private final EhcacheEvictionListener evictionListener;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();
    // Rewound event ids whose off-heap entries remain until the re-read overwrites them
    private final Map<String, Set<Integer>> staleEventIdsByTransactionId = new HashMap<>();

    public EhcacheLogMinerTransactionCache(Cache<String, EhcacheTransaction> transactionCache,
                                           Cache<String, LogMinerEvent> eventCache,
                                           EhcacheEvictionListener evictionListener) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.evictionListener = evictionListener;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public EhcacheTransaction getTransaction(String transactionId) {
        return transactionCache.get(transactionId);
    }

    @Override
    public void addTransaction(EhcacheTransaction transaction) {
        transactionCache.put(transaction.getTransactionId(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
        final TreeSet<Integer> previousEventIds = eventIdsByTransactionId.put(transaction.getTransactionId(), new TreeSet<>());
        if (previousEventIds != null) {
            transactionEvents -= previousEventIds.size();
        }
        removeStaleTransactionEvents(transaction.getTransactionId());
    }

    @Override
    public void removeTransaction(EhcacheTransaction transaction) {
        transactionCache.remove(transaction.getTransactionId());
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return eventIdsByTransactionId.containsKey(transactionId);
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
    public void eventKeys(Consumer<Stream<String>> consumer) {
        try (var stream = StreamSupport.stream(eventCache.spliterator(), false)) {
            consumer.accept(stream.map(Cache.Entry::getKey));
        }
    }

    @Override
    public void forEachEvent(EhcacheTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            for (int eventId : events) {
                if (!predicate.test(eventCache.get(transaction.getEventId(eventId)))) {
                    break;
                }
            }
        }
    }

    @Override
    public EhcacheTransaction getAndRemoveTransaction(String transactionId) {
        final EhcacheTransaction transaction = getTransaction(transactionId);
        if (transaction != null) {
            transactionCache.remove(transactionId);
        }
        return transaction;
    }

    @Override
    public void addTransactionEvent(EhcacheTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
        checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);
        final TreeSet<Integer> eventIds = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (eventIds.add(eventKey)) {
            transactionEvents++;
        }
        final Set<Integer> staleEventIds = staleEventIdsByTransactionId.get(transaction.getTransactionId());
        if (staleEventIds != null && staleEventIds.remove(eventKey) && staleEventIds.isEmpty()) {
            staleEventIdsByTransactionId.remove(transaction.getTransactionId());
        }

        if (event instanceof RollbackToSavepointEvent) {
            final Iterator<LogMinerEventEntry> reverseIterator = new LogMinerEventEntryIterator(
                    eventIds.descendingIterator(), id -> eventCache.get(transaction.getEventId(id)));
            final LogMinerEventEntryRange range = findRolledBackRange(transaction.getTransactionId(), reverseIterator);
            if (range != null) {
                final Iterator<Integer> forwardIterator = eventIds.subSet(range.start().eventId(), range.end().eventId()).iterator();
                while (forwardIterator.hasNext()) {
                    eventCache.remove(transaction.getEventId(forwardIterator.next()));
                    forwardIterator.remove();
                    transactionEvents--;
                }
            }
        }
    }

    @Override
    public void removeTransactionEvents(EhcacheTransaction transaction) {
        final var events = eventIdsByTransactionId.remove(transaction.getTransactionId());
        if (events != null) {
            eventCache.removeAll(events
                    .stream()
                    .map(transaction::getEventId)
                    .collect(Collectors.toSet()));
            transactionEvents -= events.size();
        }
        removeStaleTransactionEvents(transaction.getTransactionId());
    }

    @Override
    public boolean containsTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        // Uses the highest event key ever assigned rather than checking for presence directly
        // since a partial rollback may have removed the event's entry from the cache.
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        return events != null && !events.isEmpty() && events.last() >= eventKey;
    }

    @Override
    public int getTransactionEventCount(EhcacheTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public void rewindTransaction(EhcacheTransaction transaction, int eventId) {
        final TreeSet<Integer> eventIds = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (eventIds != null) {
            final SortedSet<Integer> rewoundEventIds = eventIds.tailSet(eventId);
            if (!rewoundEventIds.isEmpty()) {
                staleEventIdsByTransactionId.computeIfAbsent(transaction.getTransactionId(), id -> new HashSet<>()).addAll(rewoundEventIds);
                transactionEvents -= rewoundEventIds.size();
                rewoundEventIds.clear();
            }
        }
        transaction.rewindTo(eventId);
        syncTransaction(transaction);
    }

    @Override
    public void clear() {
        transactionCache.clear();
        eventCache.clear();
        eventIdsByTransactionId.clear();
        staleEventIdsByTransactionId.clear();
        transactionEvents = 0;
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
        transactionCache.put(transaction.getTransactionId(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the off-heap caches contained data on start-up
        final List<String> staleEventKeys = new ArrayList<>();
        final Map<String, Integer> numberOfEventsByTransactionId = new HashMap<>();
        eventKeys(keyStream -> {
            keyStream.map(k -> k.split("-", 2))
                    .filter(parts -> parts.length == 2)
                    .forEach(parts -> {
                        final String transactionId = parts[0];
                        final int eventId = Integer.parseInt(parts[1]);
                        final int numberOfEvents = numberOfEventsByTransactionId.computeIfAbsent(transactionId, id -> {
                            final EhcacheTransaction transaction = transactionCache.get(id);
                            return transaction == null ? -1 : transaction.getNumberOfEvents();
                        });
                        if (numberOfEvents < 0) {
                            return;
                        }
                        if (eventId >= numberOfEvents) {
                            // Left behind by a rewind that the re-read had not yet overwritten
                            staleEventKeys.add(toEventKey(transactionId, eventId));
                        }
                        else if (eventIdsByTransactionId.computeIfAbsent(transactionId, k -> new TreeSet<>()).add(eventId)) {
                            transactionEvents++;
                        }
                    });
        });
        eventCache.removeAll(Set.copyOf(staleEventKeys));
        // Transactions without events, such as those whose events were all rewound, still need a heap entry
        transactions(stream -> stream.forEach(t -> eventIdsByTransactionId.computeIfAbsent(t.getTransactionId(), id -> new TreeSet<>())));
    }

    private void removeStaleTransactionEvents(String transactionId) {
        final Set<Integer> staleEventIds = staleEventIdsByTransactionId.remove(transactionId);
        if (staleEventIds != null) {
            eventCache.removeAll(staleEventIds.stream().map(id -> toEventKey(transactionId, id)).collect(Collectors.toSet()));
        }
    }

    private void checkAndThrowIfEviction(String cacheName) {
        if (evictionListener.hasEvictionBeenSeen()) {
            throw new CacheCapacityExceededException(cacheName);
        }
    }
}
