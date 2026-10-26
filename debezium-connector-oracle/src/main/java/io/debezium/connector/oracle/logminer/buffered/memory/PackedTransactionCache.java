/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache.InterruptiblePredicate;
import io.debezium.connector.oracle.logminer.buffered.SavepointRollbacks;
import io.debezium.connector.oracle.logminer.buffered.TableDictionary;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * The {@link PackedTransaction}s of the heap buffer, keyed by transaction id, with the dictionary and the
 * payload encoder they share.
 *
 * @author Chris Cranford
 */
final class PackedTransactionCache {

    private final Map<String, PackedTransaction> transactions = new HashMap<>();
    private final TableDictionary tables = new TableDictionary();
    private final OraclePayloadCodec codec = new OraclePayloadCodec();
    private final int chunkSize;
    private final int growthMax;
    private final ChunkCompressor compressor;

    /**
     * @param chunkSize the capacity of a payload chunk in bytes
     * @param growthMax the most slots a transaction's metadata arrays grow by at a time
     * @param compressor the compressor that seals full chunks, or {@code null} to keep them raw
     */
    PackedTransactionCache(int chunkSize, int growthMax, ChunkCompressor compressor) {
        this.chunkSize = chunkSize;
        this.growthMax = growthMax;
        this.compressor = compressor;
    }

    PackedTransaction get(String transactionId) {
        return transactions.get(transactionId);
    }

    /**
     * Appends an event to its transaction, creating the transaction's store on its first event.
     *
     * @param transactionId the transaction the event belongs to
     * @param anchor the transaction's start SCN, used when the store is created
     * @param event the event
     */
    void append(String transactionId, Scn anchor, LogMinerRedoEvent event) {
        transactions.computeIfAbsent(transactionId, id -> new PackedTransaction(anchor, tables, codec, chunkSize, growthMax, compressor))
                .append(event);
    }

    /**
     * @return the number of events the transaction has, {@code 0} when it has none
     */
    int count(String transactionId) {
        final PackedTransaction transaction = transactions.get(transactionId);
        return transaction == null ? 0 : transaction.count();
    }

    /**
     * Reads the events of a transaction in order, leaving out the ones its partial rollbacks undid.
     */
    void forEach(String transactionId, InterruptiblePredicate<LogMinerRedoEvent> predicate) throws InterruptedException {
        final PackedTransaction transaction = transactions.get(transactionId);
        if (transaction != null) {
            transaction.forEach(SavepointRollbacks.resolve(transactionId, transaction.metadata(), transaction::get), predicate);
        }
    }

    /**
     * Drops the events of a transaction from the given position on, and the transaction's store when nothing
     * remains, since it is recreated on the next event.
     *
     * @return the number of events dropped
     */
    int truncate(String transactionId, int count) {
        final PackedTransaction transaction = transactions.get(transactionId);
        if (transaction == null) {
            return 0;
        }
        final int dropped = Math.max(0, transaction.count() - count);
        transaction.truncateTo(count);
        if (transaction.count() == 0) {
            transactions.remove(transactionId);
        }
        return dropped;
    }

    PackedTransaction remove(String transactionId) {
        return transactions.remove(transactionId);
    }

    void forEach(BiConsumer<String, PackedTransaction> consumer) {
        transactions.forEach(consumer);
    }

    int size() {
        return transactions.size();
    }

    boolean isEmpty() {
        return transactions.isEmpty();
    }

    TableDictionary tables() {
        return tables;
    }

    void clear() {
        transactions.clear();
        tables.clear();
    }
}
