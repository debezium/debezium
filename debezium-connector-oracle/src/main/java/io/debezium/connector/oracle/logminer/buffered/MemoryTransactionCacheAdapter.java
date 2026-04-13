/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import io.debezium.connector.oracle.logminer.buffered.memory.MemoryLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * An adapter that maps provider transactions to internal MemoryTransaction instances.
 * Single-threaded since LogMiner processing is single-threaded.
 */
public class MemoryTransactionCacheAdapter<T extends Transaction> {

    private final MemoryLogMinerTransactionCache delegate;
    private final Map<String, MemoryTransaction> transactionsById = new HashMap<>();

    public MemoryTransactionCacheAdapter() {
        this(new MemoryLogMinerTransactionCache());
    }

    public MemoryTransactionCacheAdapter(MemoryLogMinerTransactionCache delegate) {
        this.delegate = delegate == null ? new MemoryLogMinerTransactionCache() : delegate;
    }

    public void addTransaction(T transaction) {
        MemoryTransaction memTx = new MemoryTransaction(transaction);
        transactionsById.put(transaction.getTransactionId(), memTx);
        delegate.addTransaction(memTx);
    }

    /** For compatibility, allow adding a MemoryTransaction directly. */
    public void addTransaction(MemoryTransaction memTx) {
        transactionsById.put(memTx.getTransactionId(), memTx);
        delegate.addTransaction(memTx);
    }

    public MemoryTransaction getTransaction(String transactionId) {
        return transactionsById.get(transactionId);
    }

    public void removeTransaction(MemoryTransaction memTx) {
        if (memTx == null) {
            return;
        }
        transactionsById.remove(memTx.getTransactionId());
        delegate.removeTransaction(memTx);
    }

    public void removeTransaction(T transaction) {
        String id = transaction.getTransactionId();
        MemoryTransaction memTx = transactionsById.remove(id);
        if (memTx != null) {
            delegate.removeTransaction(memTx);
        }
    }

    public boolean containsTransaction(String transactionId) {
        return transactionsById.containsKey(transactionId);
    }

    public boolean isEmpty() {
        return transactionsById.isEmpty();
    }

    public int getTransactionCount() {
        return transactionsById.size();
    }

    public <R> R streamTransactionsAndReturn(Function<Stream<MemoryTransaction>, R> consumer) {
        return consumer.apply(transactionsById.values().stream());
    }

    public void transactions(Consumer<Stream<MemoryTransaction>> consumer) {
        consumer.accept(transactionsById.values().stream());
    }

    public void eventKeys(Consumer<Stream<String>> consumer) {
        delegate.eventKeys(consumer);
    }

    public void forEachEvent(MemoryTransaction transaction,
                             io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache.InterruptiblePredicate<LogMinerEvent> predicate)
            throws InterruptedException {
        delegate.forEachEvent(transaction, predicate);
    }

    public LogMinerEvent getTransactionEvent(MemoryTransaction transaction, int eventKey) {
        return delegate.getTransactionEvent(transaction, eventKey);
    }

    public MemoryTransaction getAndRemoveTransaction(String transactionId) {
        MemoryTransaction tx = transactionsById.remove(transactionId);
        if (tx != null) {
            delegate.removeTransaction(tx);
        }
        return tx;
    }

    public void addTransactionEvent(MemoryTransaction transaction, int eventKey, LogMinerEvent event) {
        delegate.addTransactionEvent(transaction, eventKey, event);
    }

    public void removeTransactionEvents(MemoryTransaction transaction) {
        delegate.removeTransactionEvents(transaction);
        transactionsById.remove(transaction.getTransactionId());
    }

    public boolean removeTransactionEventWithRowId(MemoryTransaction transaction, String rowId) {
        return delegate.removeTransactionEventWithRowId(transaction, rowId);
    }

    public boolean containsTransactionEvent(MemoryTransaction transaction, int eventKey) {
        return delegate.containsTransactionEvent(transaction, eventKey);
    }

    public boolean removeTransactionEventWithEventKey(MemoryTransaction transaction, int eventKey) {
        return delegate.removeTransactionEventWithEventKey(transaction, eventKey);
    }

    public int getTransactionEventCount(MemoryTransaction transaction) {
        return delegate.getTransactionEventCount(transaction);
    }

    public int getTransactionEvents() {
        return delegate.getTransactionEvents();
    }

    public void clear() {
        transactionsById.clear();
        delegate.clear();
    }

    public void syncTransaction(MemoryTransaction transaction) {
        delegate.syncTransaction(transaction);
    }
}
