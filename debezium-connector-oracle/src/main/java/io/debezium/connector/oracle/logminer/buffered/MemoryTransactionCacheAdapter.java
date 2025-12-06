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

    private final MemoryLogMinerTransactionCache inner;
    private final Map<String, MemoryTransaction> txMap = new HashMap<>();

    public MemoryTransactionCacheAdapter() {
        this(new MemoryLogMinerTransactionCache());
    }

    public MemoryTransactionCacheAdapter(MemoryLogMinerTransactionCache inner) {
        this.inner = inner == null ? new MemoryLogMinerTransactionCache() : inner;
    }

    public void addTransaction(T transaction) {
        MemoryTransaction memTx = new MemoryTransaction(transaction);
        txMap.put(transaction.getTransactionId(), memTx);
        inner.addTransaction(memTx);
    }

    /** For compatibility, allow adding a MemoryTransaction directly. */
    public void addTransaction(MemoryTransaction memTx) {
        txMap.put(memTx.getTransactionId(), memTx);
        inner.addTransaction(memTx);
    }

    public MemoryTransaction getTransaction(String transactionId) {
        return txMap.get(transactionId);
    }

    public void removeTransaction(MemoryTransaction memTx) {
        if (memTx == null) {
            return;
        }
        txMap.remove(memTx.getTransactionId());
        inner.removeTransaction(memTx);
    }

    public void removeTransaction(T transaction) {
        String id = transaction.getTransactionId();
        MemoryTransaction memTx = txMap.remove(id);
        if (memTx != null) {
            inner.removeTransaction(memTx);
        }
    }

    public boolean containsTransaction(String transactionId) {
        return txMap.containsKey(transactionId);
    }

    public boolean isEmpty() {
        return txMap.isEmpty();
    }

    public int getTransactionCount() {
        return txMap.size();
    }

    public <R> R streamTransactionsAndReturn(Function<Stream<MemoryTransaction>, R> consumer) {
        return consumer.apply(txMap.values().stream());
    }

    public void transactions(Consumer<Stream<MemoryTransaction>> consumer) {
        consumer.accept(txMap.values().stream());
    }

    public void eventKeys(Consumer<Stream<String>> consumer) {
        inner.eventKeys(consumer);
    }

    public void forEachEvent(MemoryTransaction transaction,
                             io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache.InterruptiblePredicate<LogMinerEvent> predicate)
            throws InterruptedException {
        inner.forEachEvent(transaction, predicate);
    }

    public LogMinerEvent getTransactionEvent(MemoryTransaction transaction, int eventKey) {
        return inner.getTransactionEvent(transaction, eventKey);
    }

    public MemoryTransaction getAndRemoveTransaction(String transactionId) {
        MemoryTransaction tx = txMap.remove(transactionId);
        if (tx != null) {
            inner.removeTransaction(tx);
        }
        return tx;
    }

    public void addTransactionEvent(MemoryTransaction transaction, int eventKey, LogMinerEvent event) {
        inner.addTransactionEvent(transaction, eventKey, event);
    }

    public void removeTransactionEvents(MemoryTransaction transaction) {
        inner.removeTransactionEvents(transaction);
        txMap.remove(transaction.getTransactionId());
    }

    public boolean removeTransactionEventWithRowId(MemoryTransaction transaction, String rowId) {
        return inner.removeTransactionEventWithRowId(transaction, rowId);
    }

    public boolean containsTransactionEvent(MemoryTransaction transaction, int eventKey) {
        return inner.containsTransactionEvent(transaction, eventKey);
    }

    public boolean removeTransactionEventWithEventKey(MemoryTransaction transaction, int eventKey) {
        return inner.removeTransactionEventWithEventKey(transaction, eventKey);
    }

    public int getTransactionEventCount(MemoryTransaction transaction) {
        return inner.getTransactionEventCount(transaction);
    }

    public int getTransactionEvents() {
        return inner.getTransactionEvents();
    }

    public void clear() {
        txMap.clear();
        inner.clear();
    }

    public void syncTransaction(MemoryTransaction transaction) {
        inner.syncTransaction(transaction);
    }
}
