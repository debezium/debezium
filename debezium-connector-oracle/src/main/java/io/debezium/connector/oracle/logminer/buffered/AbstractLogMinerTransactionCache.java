/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An abstract implementation of {@link LogMinerTransactionCache}.
 *
 * @param <T> the transaction type
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerTransactionCache<T extends Transaction> implements LogMinerTransactionCache<T> {

    private final Set<String> abandonedTransactions = new HashSet<>();
    private final Map<String, Integer> eventCountsAtBatchStart = new HashMap<>();
    private boolean batchActive;

    // Running total of the events cached across all transactions, maintained by the implementations
    // so that it does not have to be computed by iterating all transactions on every enqueued event
    protected int transactionEvents;

    @Override
    public int getTransactionEvents() {
        return transactionEvents;
    }

    @Override
    public void abandon(T transaction) {
        abandonedTransactions.add(transaction.getTransactionId());
    }

    @Override
    public void removeAbandonedTransaction(String transactionId) {
        abandonedTransactions.remove(transactionId);
    }

    @Override
    public boolean isAbandoned(String transactionId) {
        return abandonedTransactions.contains(transactionId);
    }

    @Override
    public void resetTransactionToStart(T transaction) {
        transaction.start();
    }

    @Override
    public void beginBatch() {
        eventCountsAtBatchStart.clear();
        batchActive = true;
    }

    @Override
    public void completeBatch() {
        eventCountsAtBatchStart.clear();
        batchActive = false;
    }

    @Override
    public void discardBatch() {
        for (Map.Entry<String, Integer> entry : eventCountsAtBatchStart.entrySet()) {
            final int count = entry.getValue();
            transactionEvents -= truncateTransactionEvents(entry.getKey(), count);

            // The transaction hands out the positions, so its counter rewinds with the events; a transaction
            // the pass committed is gone already and needs nothing
            final T transaction = getTransaction(entry.getKey());
            if (transaction != null) {
                transaction.rewindEvents(count);
                syncTransaction(transaction);
            }
        }
        eventCountsAtBatchStart.clear();
        batchActive = false;
    }

    /**
     * Records a transaction's event count the first time an event is added to it during a mining pass.
     * Implementations call this before every append.
     *
     * @param transactionId the transaction, should not be {@code null}
     * @param eventCount the number of events the transaction has before the append
     */
    protected void markBatchStart(String transactionId, int eventCount) {
        if (batchActive) {
            eventCountsAtBatchStart.putIfAbsent(transactionId, eventCount);
        }
    }

    /**
     * Drops the events of a transaction from the given position on.
     *
     * @param transactionId the transaction, should not be {@code null}
     * @param count the number of events to keep
     * @return the number of events dropped
     */
    protected abstract int truncateTransactionEvents(String transactionId, int count);

    /**
     * The key of an event in a cache keyed per event, which is the transaction id and the event's position.
     */
    protected static String eventKey(String transactionId, int position) {
        return transactionId + "-" + position;
    }

    @Override
    public Optional<ScnDetails> getEldestTransactionScnDetailsInCache() {
        // Returning the eldest transaction would be misleading here because each cache implementation may not
        // be able to guarantee that the transactions are returned in chronological order. So instead, the
        // cache can only provide SCN details, which for multiple eldest transactions may be the same.
        return streamTransactionsAndReturn(stream -> stream.min(this::compareTransactionScnDetails)
                .map(transaction -> new ScnDetails(transaction.getStartScn(), transaction.getChangeTime())));
    }

    protected int compareTransactionScnDetails(T first, T second) {
        int scnComparison = first.getStartScn().compareTo(second.getStartScn());
        if (scnComparison != 0) {
            return scnComparison;
        }
        return first.getChangeTime().compareTo(second.getChangeTime());
    }

}
