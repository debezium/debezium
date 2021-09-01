/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.Iterator;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.Transaction;

/**
 * Generalized contract that all transaction cache implementations should implement.
 *
 * @author Chris Cranford
 */
public interface TransactionCache<I> extends AutoCloseable {
    Transaction get(String transactionId);

    void put(String transactionId, Transaction transaction);

    Transaction remove(String transactionId);

    int size();

    void clear();

    boolean isEmpty();

    Iterator<I> iterator();

    Scn getMinimumScn();

    /**
     * Reduces the memory footprint of the transaction cache, if the cache supports it.
     */
    void trimToSize();
}
