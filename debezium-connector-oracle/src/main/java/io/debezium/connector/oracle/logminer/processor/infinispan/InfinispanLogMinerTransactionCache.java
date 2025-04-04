/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.api.BasicCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerTransactionCache;

/**
 * A concrete implementation of {@link AbstractLogMinerTransactionCache} for Infinispan.
 *
 * @author Chris Cranford
 */
public class InfinispanLogMinerTransactionCache extends AbstractLogMinerTransactionCache<InfinispanTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanLogMinerTransactionCache.class);

    private final BasicCache<String, InfinispanTransaction> transactionCache;
    private final BasicCache<String, LogMinerEvent> eventCache;

    // Heap-backed caches for quick access to specific metadata to speed up processing
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();

    // Stores asynchronous cache put operations that read and remove operations should wait on
    private final List<CompletableFuture<?>> pendingWrites = new ArrayList<>();

    public InfinispanLogMinerTransactionCache(BasicCache<String, InfinispanTransaction> transactionCache, BasicCache<String, LogMinerEvent> eventCache) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;

        primeEventCountsByTransactionId();
    }

    @Override
    public InfinispanTransaction getTransaction(String transactionId) {
        waitForAllWrites();
        return transactionCache.get(transactionId);
    }

    @Override
    public void addTransaction(InfinispanTransaction transaction) {
        putAsync(transactionCache, transaction.getTransactionId(), transaction);
        eventIdsByTransactionId.put(transaction.getTransactionId(), new TreeSet<>());
    }

    @Override
    public void removeTransaction(InfinispanTransaction transaction) {
        waitForAllWrites();
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
        waitForAllWrites();
        try (Stream<InfinispanTransaction> stream = transactionCache.values().stream()) {
            return consumer.apply(stream);
        }
    }

    @Override
    public void transactions(Consumer<Stream<InfinispanTransaction>> consumer) {
        waitForAllWrites();
        try (Stream<InfinispanTransaction> stream = transactionCache.values().stream()) {
            consumer.accept(stream);
        }
    }

    @Override
    public void eventKeys(Consumer<Stream<String>> consumer) {
        waitForAllWrites();
        try (Stream<String> stream = eventCache.keySet().stream()) {
            consumer.accept(stream);
        }
    }

    @Override
    public void forEachEvent(InfinispanTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        try (var stream = eventIdsByTransactionId.get(transaction.getTransactionId()).stream()) {
            final Iterator<Integer> iterator = stream.iterator();
            while (iterator.hasNext()) {
                final LogMinerEvent event = getTransactionEvent(transaction, iterator.next());
                if (!predicate.test(event)) {
                    break;
                }
            }
        }
    }

    @Override
    public LogMinerEvent getTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        waitForAllWrites();
        return eventCache.get(transaction.getEventId(eventKey));
    }

    @Override
    public InfinispanTransaction getAndRemoveTransaction(String transactionId) {
        waitForAllWrites();
        // Intentionally blocking
        return transactionCache.remove(transactionId);
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerEvent event) {
        putAsync(eventCache, transaction.getEventId(eventKey), event);
        eventIdsByTransactionId.get(transaction.getTransactionId()).add(eventKey);
    }

    @Override
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        waitForAllWrites();

        eventIdsByTransactionId.get(transaction.getTransactionId())
                .descendingSet()
                .stream()
                .map(transaction::getEventId)
                .forEach(eventCache::remove);

        eventIdsByTransactionId.remove(transaction.getTransactionId());
    }

    @Override
    public boolean removeTransactionEventWithRowId(InfinispanTransaction transaction, String rowId) {
        waitForAllWrites();

        final TreeSet<Integer> eventIds = eventIdsByTransactionId.get(transaction.getTransactionId());
        return eventIds.descendingSet().stream()
                .map(i -> Map.entry(i, transaction.getEventId(i)))
                .filter(entry -> {
                    final LogMinerEvent event = eventCache.get(entry.getValue());
                    return event != null && event.getRowId().equals(rowId);
                })
                .findFirst()
                .map(entry -> {
                    eventCache.remove(entry.getValue());
                    eventIds.remove(entry.getKey());
                    return true;
                })
                .orElse(false);
    }

    @Override
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        return eventIdsByTransactionId.get(transaction.getTransactionId()).contains(eventKey);
    }

    @Override
    public int getTransactionEventCount(InfinispanTransaction transaction) {
        return eventIdsByTransactionId.get(transaction.getTransactionId()).size();
    }

    @Override
    public int getTransactionEvents() {
        return eventIdsByTransactionId.values().stream().mapToInt(Set::size).sum();
    }

    @Override
    public void clear() {
        // Wait for all writes before we clear the cache.
        waitForAllWrites();

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
        putAsync(transactionCache, transaction.getTransactionId(), transaction);
    }

    @Override
    public void close() throws Exception {
        // Makes sure during shutdown that all pending writes are flushed
        waitForAllWrites();
    }

    private void primeEventCountsByTransactionId() {
        // Primes the heap-based cache if the Infinispan disk caches contained data on start-up
        transactions(stream -> stream.forEach(transaction -> {
            try (Stream<String> keyStream = eventCache.keySet().stream()) {
                eventIdsByTransactionId.put(
                        transaction.getTransactionId(),
                        keyStream.map(k -> k.split("-", 2))
                                .filter(parts -> parts.length == 2)
                                .filter(parts -> parts[0].equals(transaction.getTransactionId()))
                                .map(parts -> Integer.parseInt(parts[1]))
                                .collect(Collectors.toCollection(TreeSet::new)));
            }
        }));
    }

    private void waitForAllWrites() {
        for (CompletableFuture<?> future : pendingWrites) {
            try {
                future.get();
            }
            catch (Exception e) {
                LOGGER.error("Failed to wait on a pending infinispan write future", e);
            }
        }
        pendingWrites.clear();
    }

    private <K, V> void putAsync(BasicCache<K, V> cache, K key, V value) {
        pendingWrites.add(cache.putAsync(key, value));
    }
}
