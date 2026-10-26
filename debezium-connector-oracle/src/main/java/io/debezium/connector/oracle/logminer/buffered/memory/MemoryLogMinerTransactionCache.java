/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.EventMetadataColumns;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * A concrete implementation of the {@link LogMinerTransactionCache} that stores transactions and events
 * in the JVM heap for fast access and management.
 * <p>
 * The events of a transaction are kept in a {@link PackedTransaction}: primitive arrays for the fields the
 * partial rollback logic reads and shared, optionally compressed, chunks for the rest, instead of an object
 * per event. An event's key is its position in the transaction and nothing is removed from a transaction
 * while it is open: a partial rollback is resolved when the transaction is read, so until then the undone
 * events still count as events in the buffer.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerTransactionCache extends AbstractLogMinerTransactionCache<MemoryTransaction> {

    private final Map<String, MemoryTransaction> transactionsByTransactionId = new HashMap<>();
    private final PackedTransactionCache events;

    /**
     * Creates the heap-based transaction cache.
     *
     * @param chunkSize the capacity of a payload chunk in bytes
     * @param growthMax the most slots a transaction's metadata arrays grow by at a time
     * @param compression whether a full payload chunk is compressed
     */
    public MemoryLogMinerTransactionCache(int chunkSize, int growthMax, boolean compression) {
        this.events = new PackedTransactionCache(chunkSize, growthMax, compression ? ChunkCompressor.lz4() : null);
    }

    @Override
    public MemoryTransaction getTransaction(String transactionId) {
        return transactionsByTransactionId.get(transactionId);
    }

    @Override
    public void addTransaction(MemoryTransaction transaction) {
        transactionsByTransactionId.put(transaction.getTransactionId(), transaction);
    }

    @Override
    public void removeTransaction(MemoryTransaction transaction) {
        transactionsByTransactionId.remove(transaction.getTransactionId());
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return transactionsByTransactionId.containsKey(transactionId);
    }

    @Override
    public boolean isEmpty() {
        return transactionsByTransactionId.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return transactionsByTransactionId.size();
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<MemoryTransaction>, R> consumer) {
        return consumer.apply(transactionsByTransactionId.values().stream());
    }

    @Override
    public void transactions(Consumer<Stream<MemoryTransaction>> consumer) {
        consumer.accept(transactionsByTransactionId.values().stream());
    }

    @Override
    public void eventKeys(Consumer<Stream<String>> consumer) {
        final List<String> keys = new ArrayList<>();
        events.forEach((transactionId, transaction) -> IntStream.range(0, transaction.count())
                .forEach(id -> keys.add(transactionId + "-" + id)));
        consumer.accept(keys.stream());
    }

    @Override
    public void forEachEvent(MemoryTransaction transaction, InterruptiblePredicate<LogMinerRedoEvent> predicate) throws InterruptedException {
        events.forEach(transaction.getTransactionId(), predicate);
    }

    @Override
    public MemoryTransaction getAndRemoveTransaction(String transactionId) {
        return transactionsByTransactionId.remove(transactionId);
    }

    @Override
    public void addTransactionEvent(MemoryTransaction transaction, int eventKey, LogMinerRedoEvent event) {
        final int count = events.count(transaction.getTransactionId());
        if (eventKey < count) {
            // Already stored, as containsTransactionEvent reports by position
            return;
        }
        if (eventKey > count) {
            throw new IllegalArgumentException("Event key " + eventKey + " is not the next position of transaction " + transaction.getTransactionId());
        }
        events.append(transaction.getTransactionId(), transaction.getStartScn(), event);
        transactionEvents++;
    }

    @Override
    public void removeTransactionEvents(MemoryTransaction transaction) {
        final PackedTransaction packed = events.remove(transaction.getTransactionId());
        if (packed != null) {
            transactionEvents -= packed.count();
        }
    }

    @Override
    public boolean containsTransactionEvent(MemoryTransaction transaction, int eventKey) {
        // Positions are never reused, so an event is present when its position was assigned
        final PackedTransaction packed = events.get(transaction.getTransactionId());
        return packed != null && packed.count() > eventKey;
    }

    @Override
    public EventMetadataColumns getEventMetadata(MemoryTransaction transaction) {
        final PackedTransaction packed = events.get(transaction.getTransactionId());
        return packed == null ? null : packed.metadata();
    }

    @Override
    public int getTransactionEventCount(MemoryTransaction transaction) {
        final PackedTransaction packed = events.get(transaction.getTransactionId());
        return packed == null ? 0 : packed.count();
    }

    @Override
    public void clear() {
        transactionsByTransactionId.clear();
        events.clear();
        transactionEvents = 0;
    }

    @Override
    public Optional<ScnDetails> getEldestTransactionScnDetailsInCache() {
        // Called for every commit, so a plain loop over the transactions is used rather than a stream
        MemoryTransaction eldest = null;
        for (MemoryTransaction transaction : transactionsByTransactionId.values()) {
            if (eldest == null || compareTransactionScnDetails(transaction, eldest) < 0) {
                eldest = transaction;
            }
        }
        return eldest == null ? Optional.empty() : Optional.of(new ScnDetails(eldest.getStartScn(), eldest.getChangeTime()));
    }

    @Override
    public void syncTransaction(MemoryTransaction transaction) {
        // Changing the heap instance is sufficient, therefore this is a no-op
    }
}
