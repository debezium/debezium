/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commons.api.BasicCache;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.EventMetadataColumns;
import io.debezium.connector.oracle.logminer.buffered.SavepointRollbacks;
import io.debezium.connector.oracle.logminer.buffered.TableDictionary;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * A concrete implementation of the {@link AbstractLogMinerTransactionCache} for Infinispan.
 * <p>
 * Each event is one entry of the events cache, keyed by transaction id and position. The fields the partial
 * rollback logic reads are kept on the heap in {@link EventMetadataColumns}, one per transaction, so an undo
 * is resolved when the transaction is read without reading any entry, and only the surviving entries are read.
 *
 * @author Chris Cranford
 */
public class InfinispanLogMinerTransactionCache extends AbstractLogMinerTransactionCache<InfinispanTransaction> {

    private final BasicCache<String, InfinispanTransaction> transactionCache;
    private final BasicCache<String, LogMinerRedoEvent> eventCache;
    private final int growthMax;

    // Heap-backed metadata for quick access to what the rollback logic reads
    private final Map<String, EventMetadataColumns> metadataByTransactionId = new HashMap<>();
    private final TableDictionary tables = new TableDictionary();

    public InfinispanLogMinerTransactionCache(BasicCache<String, InfinispanTransaction> transactionCache, BasicCache<String, LogMinerRedoEvent> eventCache,
                                              int growthMax) {
        this.transactionCache = transactionCache;
        this.eventCache = eventCache;
        this.growthMax = growthMax;
    }

    @Override
    public InfinispanTransaction getTransaction(String transactionId) {
        return transactionCache.get(transactionId);
    }

    @Override
    public void addTransaction(InfinispanTransaction transaction) {
        transactionCache.put(transaction.getTransactionId(), transaction);
        final EventMetadataColumns previous = metadataByTransactionId.put(transaction.getTransactionId(), new EventMetadataColumns(tables, growthMax));
        if (previous != null) {
            transactionEvents -= previous.count();
        }
    }

    @Override
    public void removeTransaction(InfinispanTransaction transaction) {
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
    public void forEachEvent(InfinispanTransaction transaction, InterruptiblePredicate<LogMinerRedoEvent> predicate) throws InterruptedException {
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
    public InfinispanTransaction getAndRemoveTransaction(String transactionId) {
        // Intentionally blocking
        return transactionCache.remove(transactionId);
    }

    @Override
    public void addTransactionEvent(InfinispanTransaction transaction, int eventKey, LogMinerRedoEvent event) {
        final String transactionId = transaction.getTransactionId();
        final EventMetadataColumns metadata = metadataByTransactionId.get(transactionId);
        if (eventKey < metadata.count()) {
            // Already stored, as containsTransactionEvent reports by position; the entry is refreshed
            eventCache.put(eventKey(transactionId, eventKey), event);
            return;
        }

        if (eventKey > metadata.count()) {
            throw new IllegalArgumentException("Event key " + eventKey + " is not the next position of transaction " + transactionId);
        }

        eventCache.put(eventKey(transactionId, eventKey), event);

        metadata.append(event);
        transactionEvents++;
    }

    @Override
    public void removeTransactionEvents(InfinispanTransaction transaction) {
        final EventMetadataColumns metadata = metadataByTransactionId.remove(transaction.getTransactionId());
        if (metadata != null) {
            removeEvents(transaction.getTransactionId(), 0, metadata.count());
            transactionEvents -= metadata.count();
        }
    }

    @Override
    public boolean containsTransactionEvent(InfinispanTransaction transaction, int eventKey) {
        // Positions are never reused, so an event is present when its position was assigned
        final EventMetadataColumns metadata = metadataByTransactionId.get(transaction.getTransactionId());
        return metadata != null && metadata.count() > eventKey;
    }

    @Override
    public EventMetadataColumns getEventMetadata(InfinispanTransaction transaction) {
        return metadataByTransactionId.get(transaction.getTransactionId());
    }

    @Override
    public int getTransactionEventCount(InfinispanTransaction transaction) {
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

    private void removeEvents(String transactionId, int from, int toExclusive) {
        // Removed from the newest, so that an interrupted removal leaves a contiguous prefix
        for (int id = toExclusive - 1; id >= from; id--) {
            eventCache.remove(eventKey(transactionId, id));
        }
    }
}
