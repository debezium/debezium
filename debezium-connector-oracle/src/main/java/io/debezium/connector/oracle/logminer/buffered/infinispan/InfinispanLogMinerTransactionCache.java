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
import io.debezium.connector.oracle.logminer.events.RollbackToSavepointEvent;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Infinispan.
 *
 * @author Chris Cranford
 */
public class InfinispanLogMinerTransactionCache extends AbstractLogMinerTransactionCache<InfinispanTransaction> {

    private final BasicCache<Integer, InfinispanTransaction> transactionCache;
    private final BasicCache<Long, LogMinerEvent> eventCache;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<Integer, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();

    public InfinispanLogMinerTransactionCache(BasicCache<Integer, InfinispanTransaction> transactionCache, BasicCache<Long, LogMinerEvent> eventCache) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public InfinispanTransaction getTransaction(String transactionId) {
        return checkSqn(transactionId, transactionCache.get(getUsnSlt(transactionId)));
    }

    @Override
    public void addTransaction(InfinispanTransaction transaction) {
        transactionCache.put(transaction.getUsnSlt(), transaction);
        eventIdsByTransactionId.put(transaction.getUsnSlt(), new TreeSet<>());
    }

    @Override
    public void removeTransaction(InfinispanTransaction transaction) {
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
    public void eventKeys(Consumer<Stream<Long>> consumer) {
        try (Stream<Long> stream = eventCache.keySet().stream()) {
            consumer.accept(stream);
        }
    }

    @Override
    public void forEachEvent(InfinispanTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
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
    public LogMinerEvent getTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        return eventCache.get(transaction.getEventId(eventKey));
    }

    @Override
    public InfinispanTransaction getAndRemoveTransaction(String transactionId) {
        // Intentionally blocking
        return transactionCache.remove(getUsnSlt(transactionId));
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
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
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getUsnSlt());
        if (events != null) {
            events.descendingSet().stream().map(transaction::getEventId).forEach(eventCache::remove);
        }
        eventIdsByTransactionId.remove(transaction.getUsnSlt());
    }

    @Override
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        // Uses the highest event key ever assigned rather than checking for presence directly
        // since a partial rollback may have removed the event's entry from the cache.
        final var events = eventIdsByTransactionId.get(transaction.getUsnSlt());
        return events != null && !events.isEmpty() && events.last() >= eventKey;
    }

    @Override
    public int getTransactionEventCount(InfinispanTransaction transaction) {
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
        transactionCache.put(transaction.getUsnSlt(), transaction);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        // Primes the heap-based cache if the Infinispan disk caches contained data on start-up
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
}
