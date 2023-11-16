package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.HashMap;
import java.util.Map;

class InMemoryPendingTransactionsCache {
    /***
     * Map of transaction ids to the number of events in cache
     */
    private Map<String, Integer> pendingTransactionInEventsCache = new HashMap<>();

    boolean contains(String transactionId) {
        return pendingTransactionInEventsCache.containsKey(transactionId);
    }

    Integer getNumPending(String transactionId) {
        Integer i = pendingTransactionInEventsCache.get(transactionId);
        if (i == null) {
            return 0;
        }
        else {
            return i;
        }
    }

    String putOrIncrement(String transactionId) {
        Integer i = pendingTransactionInEventsCache.get(transactionId);
        if (i == null) {
            pendingTransactionInEventsCache.put(transactionId, 1);
        }
        else {
            pendingTransactionInEventsCache.put(transactionId, i + 1);
        }
        return transactionId;
    }

    public void initKey(String transactionId, int count) {
        pendingTransactionInEventsCache.put(transactionId, count);
    }

    public Integer remove(String transactionId) {
        return pendingTransactionInEventsCache.remove(transactionId);
    }

}
