/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.ehcache.Cache;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.CacheProvider;
import io.debezium.connector.oracle.logminer.buffered.EventMetadataColumns;
import io.debezium.connector.oracle.logminer.buffered.SavepointRollbacks;
import io.debezium.connector.oracle.logminer.buffered.TableDictionary;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * A concrete implementation of the {@link AbstractLogMinerTransactionCache} for Ehcache.
 * <p>
 * Each event is one entry of the events cache, keyed by transaction id and position. The fields the partial
 * rollback logic reads are kept on the heap in {@link EventMetadataColumns}, one per transaction, so an undo
 * is resolved when the transaction is read without reading any entry, and only the surviving entries are read.
 *
 * @author Chris Cranford
 */
public class EhcacheLogMinerTransactionCache extends AbstractLogMinerTransactionCache<EhcacheTransaction> {

    private final Cache<String, EhcacheTransaction> transactionCache;
    private final Cache<String, LogMinerRedoEvent> eventCache;
    private final EhcacheEvictionListener evictionListener;
    private final int growthMax;

    // Heap-backed metadata for quick access to what the rollback logic reads
    private final Map<String, EventMetadataColumns> metadataByTransactionId = new HashMap<>();
    private final TableDictionary tables = new TableDictionary();

    public EhcacheLogMinerTransactionCache(Cache<String, EhcacheTransaction> transactionCache,
                                           Cache<String, LogMinerRedoEvent> eventCache,
                                           EhcacheEvictionListener evictionListener,
                                           int growthMax) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.evictionListener = evictionListener;
        this.growthMax = growthMax;
    }

    @Override
    public EhcacheTransaction getTransaction(String transactionId) {
        return transactionCache.get(transactionId);
    }

    @Override
    public void addTransaction(EhcacheTransaction transaction) {
        transactionCache.put(transaction.getTransactionId(), transaction);
        checkAndThrowIfEviction(CacheProvider.TRANSACTIONS_CACHE_NAME);
        final EventMetadataColumns previous = metadataByTransactionId.put(transaction.getTransactionId(), new EventMetadataColumns(tables, growthMax));
        if (previous != null) {
            transactionEvents -= previous.count();
        }
    }

    @Override
    public void removeTransaction(EhcacheTransaction transaction) {
        transactionCache.remove(transaction.getTransactionId());
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return metadataByTransactionId.containsKey(transactionId);
    }

    @Override
    public boolean isEmpty() {
        return metadataByTransactionId.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return metadataByTransactionId.size();
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
    public void forEachEvent(EhcacheTransaction transaction, InterruptiblePredicate<LogMinerRedoEvent> predicate) throws InterruptedException {
        final String transactionId = transaction.getTransactionId();
        final EventMetadataColumns metadata = metadataByTransactionId.get(transactionId);
        if (metadata != null) {
            final BitSet skipped = SavepointRollbacks.resolve(transactionId, metadata, id -> eventCache.get(eventKey(transactionId, id)));
            for (int id = skipped.nextClearBit(0); id < metadata.count(); id = skipped.nextClearBit(id + 1)) {
                if (!predicate.test(eventCache.get(eventKey(transactionId, id)))) {
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
    public void addTransactionEvent(EhcacheTransaction transaction, int eventKey, LogMinerRedoEvent event) {
        final String transactionId = transaction.getTransactionId();
        final EventMetadataColumns metadata = metadataByTransactionId.get(transactionId);
        if (eventKey < metadata.count()) {
            // Already stored, as containsTransactionEvent reports by position; the entry is refreshed
            eventCache.put(eventKey(transactionId, eventKey), event);
            checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);
            return;
        }

        if (eventKey > metadata.count()) {
            throw new IllegalArgumentException("Event key " + eventKey + " is not the next position of transaction " + transactionId);
        }

        markBatchStart(transactionId, metadata.count());
        eventCache.put(eventKey(transactionId, eventKey), event);

        checkAndThrowIfEviction(CacheProvider.EVENTS_CACHE_NAME);

        metadata.append(event);
        transactionEvents++;
    }

    @Override
    public void removeTransactionEvents(EhcacheTransaction transaction) {
        final EventMetadataColumns metadata = metadataByTransactionId.remove(transaction.getTransactionId());
        if (metadata != null) {
            eventCache.removeAll(eventKeys(transaction.getTransactionId(), 0, metadata.count()));
            transactionEvents -= metadata.count();
        }
    }

    @Override
    protected int truncateTransactionEvents(String transactionId, int count) {
        final EventMetadataColumns metadata = metadataByTransactionId.get(transactionId);
        if (metadata == null || count >= metadata.count()) {
            return 0;
        }
        final int dropped = metadata.count() - count;
        eventCache.removeAll(eventKeys(transactionId, count, metadata.count()));
        metadata.truncateTo(count);
        return dropped;
    }

    @Override
    public boolean containsTransactionEvent(EhcacheTransaction transaction, int eventKey) {
        // Positions are never reused, so an event is present when its position was assigned
        final EventMetadataColumns metadata = metadataByTransactionId.get(transaction.getTransactionId());
        return metadata != null && metadata.count() > eventKey;
    }

    @Override
    public EventMetadataColumns getEventMetadata(EhcacheTransaction transaction) {
        return metadataByTransactionId.get(transaction.getTransactionId());
    }

    @Override
    public int getTransactionEventCount(EhcacheTransaction transaction) {
        final EventMetadataColumns metadata = metadataByTransactionId.get(transaction.getTransactionId());
        return metadata == null ? 0 : metadata.count();
    }

    @Override
    public void clear() {
        transactionCache.clear();
        eventCache.clear();
        metadataByTransactionId.clear();
        tables.clear();
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

    private static Set<String> eventKeys(String transactionId, int from, int toExclusive) {
        final Set<String> keys = new HashSet<>();
        for (int id = from; id < toExclusive; id++) {
            keys.add(eventKey(transactionId, id));
        }
        return keys;
    }

    private void checkAndThrowIfEviction(String cacheName) {
        if (evictionListener.hasEvictionBeenSeen()) {
            throw new CacheCapacityExceededException(cacheName);
        }
    }
}
