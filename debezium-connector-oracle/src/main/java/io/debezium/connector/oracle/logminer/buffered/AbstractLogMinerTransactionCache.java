/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.HashSet;
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
    public Optional<ScnDetails> getEldestTransactionScnDetailsInCache() {
        // Returning the eldest transaction would be misleading here because each cache implementation may not
        // be able to guarantee that the transactions are returned in chronological order. So instead, the
        // cache can only provide SCN details, which for multiple eldest transactions may be the same.
        return streamTransactionsAndReturn(stream -> stream.min(this::compareTransactionScnDetails)
                .map(transaction -> new ScnDetails(transaction.getStartScn(), transaction.getChangeTime())));
    }

    private int compareTransactionScnDetails(T first, T second) {
        int scnComparison = first.getStartScn().compareTo(second.getStartScn());
        if (scnComparison != 0) {
            return scnComparison;
        }
        return first.getChangeTime().compareTo(second.getChangeTime());
    }
}
