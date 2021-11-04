/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;

/**
 * A {@link TransactionCache} implementation that uses a JVM heap backed {@code HashMap}.
 *
 * @author Chris Cranford
 */
public class MemoryTransactionCache implements TransactionCache<MemoryTransaction, Map.Entry<String, MemoryTransaction>> {

    public final Map<String, MemoryTransaction> cache = new HashMap<>();

    @Override
    public MemoryTransaction get(String transactionId) {
        return cache.get(transactionId);
    }

    @Override
    public void put(String transactionId, MemoryTransaction transaction) {
        cache.put(transactionId, transaction);
    }

    @Override
    public MemoryTransaction remove(String transactionId) {
        return cache.remove(transactionId);
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public Iterator<Map.Entry<String, MemoryTransaction>> iterator() {
        return cache.entrySet().iterator();
    }

    @Override
    public Scn getMinimumScn() {
        return cache.values().stream()
                .map(MemoryTransaction::getStartScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }

    @Override
    public void close() throws Exception {
        // no-op
    }
}
