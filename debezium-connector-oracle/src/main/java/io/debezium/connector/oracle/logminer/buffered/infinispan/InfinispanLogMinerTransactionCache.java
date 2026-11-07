/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

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

    private final BasicCache<String, InfinispanTransaction> transactionCache;
    private final BasicCache<String, LogMinerEvent> eventCache;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();
    // Rewound event ids whose off-heap entries remain until the re-read overwrites them
    private final Map<String, Set<Integer>> staleEventIdsByTransactionId = new HashMap<>();

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
        final TreeSet<Integer> previousEventIds = eventIdsByTransactionId.put(transaction.getTransactionId(), new TreeSet<>());
        if (previousEventIds != null) {
            transactionEvents -= previousEventIds.size();
        }
        removeStaleTransactionEvents(transaction.getTransactionId());
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
            for (int eventId : events) {
                if (!predicate.test(eventCache.get(transaction.getEventId(eventId)))) {
                    break;
                }
            }
        }
    }

    @Override
    public InfinispanTransaction getAndRemoveTransaction(String transactionId) {
        // Intentionally blocking
        return transactionCache.remove(transactionId);
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
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
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        final var events = eventIdsByTransactionId.remove(transaction.getTransactionId());
        if (events != null) {
            events.descendingSet().stream().map(transaction::getEventId).forEach(eventCache::remove);
            transactionEvents -= events.size();
        }
        removeStaleTransactionEvents(transaction.getTransactionId());
    }

    @Override
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        // Uses the highest event key ever assigned rather than checking for presence directly
        // since a partial rollback may have removed the event's entry from the cache.
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        return events != null && !events.isEmpty() && events.last() >= eventKey;
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
    public void rewindTransaction(InfinispanTransaction transaction, int eventId) {
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
                            final InfinispanTransaction transaction = transactionCache.get(id);
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
        staleEventKeys.forEach(eventCache::remove);
        // Transactions without events, such as those whose events were all rewound, still need a heap entry
        transactions(stream -> stream.forEach(t -> eventIdsByTransactionId.computeIfAbsent(t.getTransactionId(), id -> new TreeSet<>())));
    }

    private void removeStaleTransactionEvents(String transactionId) {
        final Set<Integer> staleEventIds = staleEventIdsByTransactionId.remove(transactionId);
        if (staleEventIds != null) {
            staleEventIds.forEach(id -> eventCache.remove(toEventKey(transactionId, id)));
        }
    }
}
