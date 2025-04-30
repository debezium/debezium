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
        eventIdsByTransactionId.put(transaction.getTransactionId(), new TreeSet<>());
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
        try (var stream = eventIdsByTransactionId.get(transaction.getTransactionId()).stream()) {
            final Iterator<Integer> iterator = stream.iterator();
            while (iterator.hasNext()) {
                final LogMinerEvent event = getTransactionEvent(transaction, iterator.next());
                if (event != null && !predicate.test(event)) {
                    break;
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
            transactionCache.remove(transactionId);
        }
        return transaction;
    }

    @Override
    public void addTransactionEvent(EhcacheTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
        checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);
        eventIdsByTransactionId.get(transaction.getTransactionId()).add(eventKey);
    }

    @Override
    public void removeTransactionEvents(EhcacheTransaction transaction) {
        eventCache.removeAll(eventIdsByTransactionId.get(transaction.getTransactionId())
                .stream()
                .map(transaction::getEventId)
                .collect(Collectors.toSet()));

        eventIdsByTransactionId.remove(transaction.getTransactionId());
    }

    @Override
    public boolean removeTransactionEventWithRowId(EhcacheTransaction transaction, String rowId) {
        final TreeSet<Integer> eventIds = eventIdsByTransactionId.get(transaction.getTransactionId());
        for (Integer eventId : eventIds.descendingSet()) {
            final String eventKey = transaction.getEventId(eventId);
            final LogMinerEvent event = eventCache.get(eventKey);
            if (event != null && event.getRowId().equals(rowId)) {
                eventCache.remove(eventKey);
                eventIds.remove(eventId);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        return eventIdsByTransactionId.get(transaction.getTransactionId()).contains(eventKey);
    }

    @Override
    public int getTransactionEventCount(EhcacheTransaction transaction) {
        return eventIdsByTransactionId.get(transaction.getTransactionId()).size();
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
        transactionCache.put(transaction.getTransactionId(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the Ehcache persistence caches contained data on start-up
        eventKeys(keyStream -> {
            keyStream.map(k -> k.split("-", 2))
                    .filter(parts -> parts.length == 2)
                    .forEach(parts -> {
                        final String transactionId = parts[0];
                        final int eventId = Integer.parseInt(parts[1]);
                        if (transactionCache.containsKey(transactionId)) {
                            eventIdsByTransactionId.computeIfAbsent(transactionId, k -> new TreeSet<>()).add(eventId);
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
