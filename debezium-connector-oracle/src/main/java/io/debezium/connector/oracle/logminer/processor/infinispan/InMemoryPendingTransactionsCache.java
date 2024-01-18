/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory pending transaction cache, used for performance reasons.
 */
class InMemoryPendingTransactionsCache {
    /***
     * Map of transaction ids to the number of events in cache
     */
    private final Map<String, Integer> pendingTransactionInEventsCache = new HashMap<>();

    Integer getNumPending(String transactionId) {
        return pendingTransactionInEventsCache.getOrDefault(transactionId, 0);
    }

    String putOrIncrement(String transactionId) {
        final Integer i = pendingTransactionInEventsCache.getOrDefault(transactionId, 0);
        pendingTransactionInEventsCache.put(transactionId, i + 1);
        return transactionId;
    }

    void decrement(String transactionId) {
        final int i = pendingTransactionInEventsCache.getOrDefault(transactionId, 0);
        if (i > 0) {
            pendingTransactionInEventsCache.put(transactionId, i - 1);
        }
    }

    public void initKey(String transactionId, int count) {
        pendingTransactionInEventsCache.put(transactionId, count);
    }

    public Integer remove(String transactionId) {
        return pendingTransactionInEventsCache.remove(transactionId);
    }

}
