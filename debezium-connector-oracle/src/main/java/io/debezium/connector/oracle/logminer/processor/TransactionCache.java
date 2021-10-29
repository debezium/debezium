/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.Iterator;

import io.debezium.connector.oracle.Scn;

/**
 * Generalized contract that all transaction cache implementations should implement.
 *
 * @author Chris Cranford
 */
public interface TransactionCache<T extends AbstractTransaction, I> extends AutoCloseable {
    T get(String transactionId);

    void put(String transactionId, T transaction);

    T remove(String transactionId);

    int size();

    void clear();

    boolean isEmpty();

    Iterator<I> iterator();

    Scn getMinimumScn();
}
