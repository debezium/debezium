/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.Transaction;
import io.debezium.connector.oracle.logminer.processor.TransactionCache;

/**
 * A {@link TransactionCache} implementation that uses a JVM heap backed {@code HashMap}.
 *
 * @author Chris Cranford
 */
public class MemoryTransactionCache implements TransactionCache<Map.Entry<String, Transaction>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTransactionCache.class);

    private final int trimFactor;
    private Map<String, Transaction> cache = new HashMap<>();
    private int maxSize;

    public MemoryTransactionCache(int trimFactor) {
        this.trimFactor = trimFactor;
    }

    @Override
    public Transaction get(String transactionId) {
        return cache.get(transactionId);
    }

    @Override
    public void put(String transactionId, Transaction transaction) {
        cache.put(transactionId, transaction);
        if (cache.size() > maxSize) {
            maxSize = cache.size();
        }
    }

    @Override
    public Transaction remove(String transactionId) {
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
    public Iterator<Map.Entry<String, Transaction>> iterator() {
        return cache.entrySet().iterator();
    }

    @Override
    public Scn getMinimumScn() {
        return cache.values().stream()
                .map(Transaction::getStartScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }

    @Override
    public void trimToSize() {
        final int factor = (int) ((float) maxSize / (float) cache.size());
        if (factor >= trimFactor) {
            // If the maximum cache size was more than x-times the current size, then force a trim/reallocate
            LOGGER.debug("Cache maxSize={} currentSize={}, trimming to size.", maxSize, cache.size());
            cache = new HashMap<>(cache);
            maxSize = cache.size();
        }
    }

    @Override
    public void close() throws Exception {
        // no-op
    }
}
