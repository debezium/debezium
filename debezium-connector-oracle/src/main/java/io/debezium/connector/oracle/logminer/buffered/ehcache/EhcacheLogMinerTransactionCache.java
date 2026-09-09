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
import io.debezium.connector.oracle.logminer.events.RowIdCodec;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Ehcache.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerTransactionCache extends AbstractLogMinerTransactionCache<EhcacheTransaction> {

    private final Cache<String, EhcacheTransaction> transactionCache;
    private final Cache<String, LogMinerEvent> eventCache;
    private final Cache<String, Boolean> rollbackCache;
    private final EhcacheEvictionListener evictionListener;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionSlt = new HashMap<>();

    public EhcacheLogMinerTransactionCache(Cache<String, EhcacheTransaction> transactionCache,
                                           Cache<String, LogMinerEvent> eventCache,
                                           Cache<String, Boolean> rollbackCache,
                                           EhcacheEvictionListener evictionListener) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.rollbackCache = rollbackCache;
        this.evictionListener = evictionListener;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public EhcacheTransaction getTransaction(String transactionId) {
        return checkTransactionSqn(transactionId, transactionCache.get(getTransactionSlt(transactionId)));
    }

    @Override
    public void addTransaction(EhcacheTransaction transaction) {
        transactionCache.put(transaction.getTransactionSlt(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
        eventIdsByTransactionSlt.put(transaction.getTransactionSlt(), new TreeSet<>());
    }

    @Override
    public void removeTransaction(EhcacheTransaction transaction) {
        transactionCache.remove(transaction.getTransactionSlt());
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return eventIdsByTransactionSlt.containsKey(getTransactionSlt(transactionId));
    }

    @Override
    public boolean isEmpty() {
        return eventIdsByTransactionSlt.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return eventIdsByTransactionSlt.size();
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
    public void forEachEvent(EhcacheTransaction transaction, LogMinerEventPredicate predicate) throws InterruptedException {
        final var events = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        if (events != null) {
            try (var stream = events.stream()) {
                final Iterator<Integer> iterator = stream.iterator();
                while (iterator.hasNext()) {
                    final String eventKey = transaction.getEventId(iterator.next());
                    if (!predicate.test(eventCache.get(eventKey), rollbackCache.containsKey(eventKey))) {
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
            removeTransaction(transaction);
        }
        return transaction;
    }

    @Override
    public void addTransactionEvent(EhcacheTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
        checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);
        eventIdsByTransactionSlt.get(transaction.getTransactionSlt()).add(eventKey);
    }

    @Override
    public void removeTransactionEvents(EhcacheTransaction transaction) {
        final var events = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        if (events != null) {
            final Set<String> keys = events.stream()
                    .map(transaction::getEventId)
                    .collect(Collectors.toSet());
            eventCache.removeAll(keys);
            rollbackCache.removeAll(keys);
        }
        eventIdsByTransactionSlt.remove(transaction.getTransactionSlt());
    }

    @Override
    public boolean rollbackTransactionEventWithRowId(EhcacheTransaction transaction, String rowId) {
        final RowIdCodec.Packed encodedRowId = RowIdCodec.encode(rowId);
        final TreeSet<Integer> eventIds = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        for (Integer eventId : eventIds.descendingSet()) {
            final String eventKey = transaction.getEventId(eventId);
            final LogMinerEvent event = eventCache.get(eventKey);
            if (event != null && event.getRowId().equals(encodedRowId) && !rollbackCache.containsKey(eventKey)) {
                rollbackCache.put(eventKey, Boolean.TRUE);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        final var events = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        if (events != null) {
            return events.contains(eventKey);
        }
        return false;
    }

    @Override
    public int getTransactionEventCount(EhcacheTransaction transaction) {
        final var events = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public int getTransactionEvents() {
        return eventIdsByTransactionSlt.values().stream().mapToInt(Set::size).sum();
    }

    @Override
    public void clear() {
        transactionCache.clear();
        eventCache.clear();
        rollbackCache.clear();
        eventIdsByTransactionSlt.clear();
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
        transactionCache.put(transaction.getTransactionSlt(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        Iterator<Cache.Entry<String, EhcacheTransaction>> transactionIt = transactionCache.iterator();
        while (transactionIt.hasNext()) {
            Cache.Entry<String, EhcacheTransaction> entry = transactionIt.next();
            String transactionSlt = getTransactionSlt(entry.getKey());
            if (!entry.getKey().equals(transactionSlt)) {
                transactionCache.put(transactionSlt, entry.getValue());
                transactionIt.remove();
            }
        }

        // Primes the heap-based cache if the Ehcache persistence caches contained data on start-up
        eventKeys(keyStream -> {
            keyStream.map(k -> k.split("-", 2))
                    .filter(parts -> parts.length == 2)
                    .forEach(parts -> {
                        final String transactionSlt = getTransactionSlt(parts[0]);

                        if (!transactionSlt.equals(parts[0])) {
                            String oldEventKey = String.join("-", parts);
                            String newEventKey = String.join("-", transactionSlt, parts[1]);

                            LogMinerEvent event = eventCache.get(oldEventKey);
                            eventCache.put(newEventKey, event);
                            eventCache.remove(oldEventKey);

                            Boolean rollback = rollbackCache.get(oldEventKey);
                            if (rollback != null) {
                                rollbackCache.put(newEventKey, rollback);
                                rollbackCache.remove(oldEventKey);
                            }
                        }

                        final int eventId = Integer.parseInt(parts[1]);
                        if (transactionCache.containsKey(transactionSlt)) {
                            eventIdsByTransactionSlt.computeIfAbsent(transactionSlt, k -> new TreeSet<>()).add(eventId);
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
