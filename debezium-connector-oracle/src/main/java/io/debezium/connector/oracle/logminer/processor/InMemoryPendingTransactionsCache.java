/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory pending transaction cache, used for performance reasons.
 */
public class InMemoryPendingTransactionsCache {
    /***
     * Map of transaction ids to the number of events in cache
     */
    private final Map<String, Integer> pendingTransactionInEventsCache = new HashMap<>();

    public int getNumPending(String transactionId) {
        return pendingTransactionInEventsCache.getOrDefault(transactionId, 0);
    }

    public void putOrIncrement(String transactionId) {
        pendingTransactionInEventsCache.compute(transactionId, (k, value) -> {
            value = value == null ? 0 : value;
            return value + 1;
        });
    }

    public void decrement(String transactionId) {
        pendingTransactionInEventsCache.compute(transactionId, (k, value) -> value == null || value == 0 ? 0 : value - 1);
    }

    public void initKey(String transactionId, int count) {
        pendingTransactionInEventsCache.put(transactionId, count);
    }

    public Integer remove(String transactionId) {
        return pendingTransactionInEventsCache.remove(transactionId);
    }

}
