/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.HashSet;
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
}
