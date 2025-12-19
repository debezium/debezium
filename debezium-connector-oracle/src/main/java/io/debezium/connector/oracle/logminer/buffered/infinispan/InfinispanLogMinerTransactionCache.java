/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commons.api.BasicCache;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Infinispan.
 *
 * @author Chris Cranford
 */
public class InfinispanLogMinerTransactionCache extends AbstractLogMinerTransactionCache<InfinispanTransaction> {

    private final BasicCache<String, InfinispanTransaction> transactionCache;
    private final BasicCache<String, LogMinerEvent> eventCache;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();

    public InfinispanLogMinerTransactionCache(BasicCache<String, InfinispanTransaction> transactionCache, BasicCache<String, LogMinerEvent> eventCache) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public InfinispanTransaction getTransaction(String transactionId) {
        return transactionCache.get(transactionId);
    }

    @Override
    public void addTransaction(InfinispanTransaction transaction) {
        transactionCache.put(transaction.getTransactionId(), transaction);
        eventIdsByTransactionId.put(transaction.getTransactionId(), new TreeSet<>());
    }

    @Override
    public void removeTransaction(InfinispanTransaction transaction) {
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
    public void eventKeys(Consumer<Stream<String>> consumer) {
        try (Stream<String> stream = eventCache.keySet().stream()) {
            consumer.accept(stream);
        }
    }

    @Override
    public void forEachEvent(InfinispanTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
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
        return eventCache.get(transaction.getEventId(eventKey));
    }

    @Override
    public InfinispanTransaction getAndRemoveTransaction(String transactionId) {
        // Intentionally blocking
        return transactionCache.remove(transactionId);
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
        eventIdsByTransactionId.get(transaction.getTransactionId()).add(eventKey);
    }

    @Override
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            events.descendingSet().stream().map(transaction::getEventId).forEach(eventCache::remove);
        }
        eventIdsByTransactionId.remove(transaction.getTransactionId());
    }

    @Override
    public boolean removeTransactionEventWithRowId(InfinispanTransaction transaction, String rowId) {
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
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            return events.contains(eventKey);
        }
        return false;
    }

    @Override
    public int getTransactionEventCount(InfinispanTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
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
        transactionCache.put(transaction.getTransactionId(), transaction);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the Infinispan disk caches contained data on start-up
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
}
