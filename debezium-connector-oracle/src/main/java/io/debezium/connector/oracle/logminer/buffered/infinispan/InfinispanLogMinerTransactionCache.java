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
import io.debezium.connector.oracle.logminer.events.RowIdCodec;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Infinispan.
 *
 * @author Chris Cranford
 */
public class InfinispanLogMinerTransactionCache extends AbstractLogMinerTransactionCache<InfinispanTransaction> {

    private final BasicCache<String, InfinispanTransaction> transactionCache;
    private final BasicCache<String, LogMinerEvent> eventCache;
    private final BasicCache<String, Boolean> rollbackCache;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionSlt = new HashMap<>();

    public InfinispanLogMinerTransactionCache(BasicCache<String, InfinispanTransaction> transactionCache,
                                              BasicCache<String, LogMinerEvent> eventCache,
                                              BasicCache<String, Boolean> rollbackCache) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.rollbackCache = rollbackCache;

        primeHeapCacheFromOffHeapCaches();
    }

    @Override
    public InfinispanTransaction getTransaction(String transactionId) {
        return checkTransactionSqn(transactionId, transactionCache.get(getTransactionSlt(transactionId)));
    }

    @Override
    public void addTransaction(InfinispanTransaction transaction) {
        transactionCache.put(transaction.getTransactionSlt(), transaction);
        eventIdsByTransactionSlt.put(transaction.getTransactionSlt(), new TreeSet<>());
    }

    @Override
    public void removeTransaction(InfinispanTransaction transaction) {
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
    public void forEachEvent(InfinispanTransaction transaction, LogMinerEventPredicate predicate) throws InterruptedException {
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
    public LogMinerEvent getTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        return eventCache.get(transaction.getEventId(eventKey));
    }

    @Override
    public InfinispanTransaction getAndRemoveTransaction(String transactionId) {
        // Intentionally blocking
        final InfinispanTransaction transaction = getTransaction(transactionId);
        if (transaction != null) {
            removeTransaction(transaction);
        }
        return transaction;
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerEvent event) {
        eventCache.put(transaction.getEventId(eventKey), event);
        eventIdsByTransactionSlt.get(transaction.getTransactionSlt()).add(eventKey);
    }

    @Override
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        final var events = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        if (events != null) {
            events.descendingSet().stream().map(transaction::getEventId).forEach(key -> {
                eventCache.remove(key);
                rollbackCache.remove(key);
            });
        }
        eventIdsByTransactionSlt.remove(transaction.getTransactionSlt());
    }

    @Override
    public boolean rollbackTransactionEventWithRowId(InfinispanTransaction transaction, String rowId) {
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
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        final var events = eventIdsByTransactionSlt.get(transaction.getTransactionSlt());
        if (events != null) {
            return events.contains(eventKey);
        }
        return false;
    }

    @Override
    public int getTransactionEventCount(InfinispanTransaction transaction) {
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
        transactionCache.put(transaction.getTransactionSlt(), transaction);
    }

    private void primeHeapCacheFromOffHeapCaches() {
        Iterator<Map.Entry<String, InfinispanTransaction>> transactionIt = transactionCache.entrySet().iterator();
        while (transactionIt.hasNext()) {
            Map.Entry<String, InfinispanTransaction> entry = transactionIt.next();
            String transactionSlt = getTransactionSlt(entry.getKey());
            if (!entry.getKey().equals(transactionSlt)) {
                transactionCache.put(transactionSlt, entry.getValue());
                transactionIt.remove();
            }
        }

        // Primes the heap-based cache if the Infinispan disk caches contained data on start-up
        eventKeys(keyStream -> {
            keyStream.map(k -> k.split("-", 2))
                    .filter(parts -> parts.length == 2)
                    .forEach(parts -> {
                        final String transactionSlt = getTransactionSlt(parts[0]);

                        if (!transactionSlt.equals(parts[0])) {
                            String oldEventKey = String.join("-", parts);
                            String newEventKey = String.join("-", transactionSlt, parts[1]);

                            eventCache.put(newEventKey, eventCache.remove(oldEventKey));

                            Boolean rollback = rollbackCache.remove(oldEventKey);
                            if (rollback != null) {
                                rollbackCache.put(newEventKey, rollback);
                            }
                        }

                        final int eventId = Integer.parseInt(parts[1]);
                        if (transactionCache.containsKey(transactionSlt)) {
                            eventIdsByTransactionSlt.computeIfAbsent(transactionSlt, k -> new TreeSet<>()).add(eventId);
                        }
                    });
        });
    }
}
