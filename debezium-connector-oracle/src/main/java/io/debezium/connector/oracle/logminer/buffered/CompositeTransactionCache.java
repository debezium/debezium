/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A composite transaction cache that manages both in-memory and spillover caches.
 * This class provides a unified interface for transaction management while handling
 * the complexity of routing events between memory and disk-based storage.
 *
 * @param <T> the transaction type
 * @author Debezium Authors
 */
public class CompositeTransactionCache<T extends Transaction> extends AbstractLogMinerTransactionCache<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeTransactionCache.class);

    /**
     * Lightweight key for tracking (rowId, eventKey) pairs in insertion order.
     */
    private record IndexKey(String rowId, Integer eventKey) {
    }

    /**
     * Lightweight index structure used only for spilled transactions. Keeps rowId -> eventKey lists
     * and minimal index bookkeeping. Created lazily when a transaction starts spilling.
     * Implements FIFO eviction using LinkedHashMap to maintain a sliding window of the most recent indexed events.
     * Single-threaded since LogMiner processing is single-threaded.
     */
    private static class SpilloverRowIdIndex {
        private final Map<String, Deque<Integer>> rowIndex = new HashMap<>();
        private final LinkedHashMap<IndexKey, Integer> insertionOrder;

        SpilloverRowIdIndex(int maxSize) {
            // LinkedHashMap with access-order=false (insertion order) and automatic eldest removal
            this.insertionOrder = new LinkedHashMap<IndexKey, Integer>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<IndexKey, Integer> eldest) {
                    if (size() > maxSize) {
                        // Remove from rowIndex when evicted from insertionOrder
                        IndexKey key = eldest.getKey();
                        Deque<Integer> eventKeys = rowIndex.get(key.rowId);
                        if (eventKeys != null) {
                            eventKeys.removeFirstOccurrence(key.eventKey);
                            if (eventKeys.isEmpty()) {
                                rowIndex.remove(key.rowId);
                            }
                        }
                        return true;
                    }
                    return false;
                }
            };
        }

        Map<String, Deque<Integer>> getRowIndex() {
            return rowIndex;
        }

        /**
         * Adds an entry to the index, tracking it in insertion order.
         * Automatic eviction happens via LinkedHashMap.removeEldestEntry().
         */
        void addEntry(String rowId, Integer eventKey) {
            rowIndex.computeIfAbsent(rowId, k -> new ArrayDeque<>()).addLast(eventKey);
            // Use lightweight record key for tracking insertion order
            insertionOrder.put(new IndexKey(rowId, eventKey), eventKey);
        }

        void clear() {
            rowIndex.clear();
            insertionOrder.clear();
        }
    }

    // Core components
    private final LogMinerTransactionCache<T> spilloverCache;
    private final SpillStrategy spillStrategy;
    private final MemoryTransactionCacheAdapter<T> memoryEventStore;
    // Single-threaded data structures (LogMiner processing is single-threaded)
    private final Map<String, T> transactionRegistry = new HashMap<>();
    private final Map<String, TransactionState> transactionStates = new HashMap<>();
    private final Map<String, SpilloverRowIdIndex> spilloverRowIdIndexes = new HashMap<>();

    // Maximum number of indexed entries per transaction before disabling index
    private final int indexThreshold;

    /**
     * Creates a new composite transaction cache.
     */
    public CompositeTransactionCache(LogMinerTransactionCache<T> spilloverCache,
                                     SpillStrategy spillStrategy) {
        this(spilloverCache, spillStrategy, Integer.MAX_VALUE);
    }

    /**
     * Creates a new composite transaction cache with index threshold.
     */
    public CompositeTransactionCache(LogMinerTransactionCache<T> spilloverCache,
                                     SpillStrategy spillStrategy,
                                     int indexThreshold) {
        this.spilloverCache = spilloverCache;
        this.spillStrategy = spillStrategy;
        this.indexThreshold = indexThreshold;

        // Create memory event store internally (adapter maps provider transactions to memory)
        this.memoryEventStore = new MemoryTransactionCacheAdapter<>();
    }

    /**
     * Helper method to check if a transaction is in the spilling state.
     */
    private boolean isSpilling(String transactionId) {
        TransactionState state = transactionStates.get(transactionId);
        return state == TransactionState.SPILLING;
    }

    /**
     * Helper method to transition a transaction to the spilling state.
     * This is the ONLY place where spilloverCache.addTransaction() is called.
     */
    private void transitionToSpilling(T transaction) {
        String transactionId = transaction.getTransactionId();

        // Update state
        transactionStates.put(transactionId, TransactionState.SPILLING);

        // Register with spillover provider
        spilloverCache.addTransaction(transaction);

        LOGGER.debug("Transaction {} transitioned to SPILLING state.", transactionId);
    }

    @Override
    public T getTransaction(String transactionId) {
        // Return from transaction registry
        return transactionRegistry.get(transactionId);
    }

    @Override
    public void addTransaction(T transaction) {
        String transactionId = transaction.getTransactionId();

        // Add to transaction registry
        transactionRegistry.put(transactionId, transaction);

        // Set initial state to MEMORY
        transactionStates.put(transactionId, TransactionState.MEMORY);

        // Register transaction with memory adapter which will create the internal MemoryTransaction
        memoryEventStore.addTransaction(transaction);

        if (spillStrategy != null) {
            spillStrategy.onTransactionCreated(transactionId);
        }
    }

    @Override
    public void removeTransaction(T transaction) {
        String transactionId = transaction.getTransactionId();

        // Remove from transaction registry
        transactionRegistry.remove(transactionId);

        // Remove state tracking
        transactionStates.remove(transactionId);

        // Remove from memory event store
        memoryEventStore.removeTransaction(transaction);

        // Remove from spillover if present
        spilloverCache.removeTransaction(transaction);

        // Remove any per-transaction index
        spilloverRowIdIndexes.remove(transactionId);

        if (spillStrategy != null) {
            spillStrategy.onTransactionRemoved(transactionId);
        }
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return transactionRegistry.containsKey(transactionId);
    }

    @Override
    public boolean isEmpty() {
        return transactionRegistry.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return transactionRegistry.size();
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<T>, R> consumer) {
        return consumer.apply(transactionRegistry.values().stream());
    }

    @Override
    public void transactions(Consumer<Stream<T>> consumer) {
        consumer.accept(transactionRegistry.values().stream());
    }

    @Override
    public void eventKeys(Consumer<Stream<String>> consumer) {
        memoryEventStore
                .eventKeys(memoryStream -> spilloverCache.eventKeys(spilloverStream -> consumer.accept(Stream.concat(memoryStream, spilloverStream))));
    }

    @Override
    public void forEachEvent(T transaction, InterruptiblePredicate<LogMinerEvent> predicate)
            throws InterruptedException {

        String transactionId = transaction.getTransactionId();

        // Track if predicate has requested early termination (using array for lambda capture)
        final boolean[] shouldContinue = { true };

        // Wrap predicate to track termination
        InterruptiblePredicate<LogMinerEvent> trackingPredicate = event -> {
            if (!shouldContinue[0]) {
                return false;
            }
            boolean result = predicate.test(event);
            if (!result) {
                shouldContinue[0] = false;
            }
            return result;
        };

        // First, iterate through in-memory events (early events in insertion order)
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        memoryEventStore.forEachEvent(memTx, trackingPredicate);

        // Then, iterate through spilled events (later events) only if predicate hasn't terminated
        if (shouldContinue[0] && isSpilling(transactionId)) {
            spilloverCache.forEachEvent(transaction, trackingPredicate);
        }
    }

    @Override
    public LogMinerEvent getTransactionEvent(T transaction, int eventKey) {
        String transactionId = transaction.getTransactionId();

        // Check if event is in memory first
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        LogMinerEvent event = memoryEventStore.getTransactionEvent(memTx, eventKey);
        if (event != null) {
            return event;
        }

        // If not in memory and transaction is spilling, check spill storage
        if (isSpilling(transactionId)) {
            return spilloverCache.getTransactionEvent(transaction, eventKey);
        }

        return null;
    }

    @Override
    public T getAndRemoveTransaction(String transactionId) {
        // Get and remove transaction from registry
        T transaction = transactionRegistry.remove(transactionId);
        if (transaction != null) {

            if (spillStrategy != null) {
                spillStrategy.onTransactionRemoved(transactionId);
            }

            return transaction;
        }
        return null;
    }

    @Override
    public void addTransactionEvent(T transaction, int eventKey, LogMinerEvent event) {
        String transactionId = transaction.getTransactionId();

        try {
            if (isSpilling(transactionId)) {
                // Already spilling - add to spillover
                spilloverCache.addTransactionEvent(transaction, eventKey, event);
                updateSpilloverRowIdIndex(transactionId, eventKey, event);
            }
            else {
                // Check if we should start spilling
                TransactionView view = new TransactionViewImpl(transactionId);
                if (spillStrategy != null && spillStrategy.shouldSpill(transactionId, event, view)) {
                    // Transition to spilling state
                    transitionToSpilling(transaction);

                    // Add event to spillover
                    spilloverCache.addTransactionEvent(transaction, eventKey, event);
                    updateSpilloverRowIdIndex(transactionId, eventKey, event);
                }
                else {
                    // Add to memory event store via adapter
                    MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
                    memoryEventStore.addTransactionEvent(memTx, eventKey, event);
                }
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to add event " + eventKey + " to transaction " + transactionId, e);
        }
    }

    private void updateSpilloverRowIdIndex(String txId, int eventKey, LogMinerEvent event) {
        // Get or create index lazily
        SpilloverRowIdIndex index = spilloverRowIdIndexes.computeIfAbsent(txId, k -> new SpilloverRowIdIndex(indexThreshold));

        try {
            String rowId = event.getRowId();

            // Add the new entry (automatic eviction via LinkedHashMap.removeEldestEntry())
            index.addEntry(rowId, eventKey);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to update spillover rowId index for transaction {}.", txId, e);
        }
    }

    @Override
    public void removeTransactionEvents(T transaction) {
        String transactionId = transaction.getTransactionId();

        // Always remove from memory event store (transaction may have events in both memory and spillover)
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        memoryEventStore.removeTransactionEvents(memTx);
        memoryEventStore.removeTransaction(memTx);

        // Remove from spillover if transaction is spilling
        if (isSpilling(transactionId)) {
            spilloverCache.removeTransactionEvents(transaction);
            spilloverCache.removeTransaction(transaction);
        }

        // Clean up index
        SpilloverRowIdIndex index = spilloverRowIdIndexes.remove(transactionId);
        if (index != null) {
            index.clear();
        }

        // Finally remove state tracking (now that we're done using isSpilling())
        transactionStates.remove(transactionId);
    }

    @Override
    public boolean removeTransactionEventWithRowId(T transaction, String rowId) {
        String transactionId = transaction.getTransactionId();

        // If transaction is spilling, try spilled index first
        if (isSpilling(transactionId)) {
            SpilloverRowIdIndex index = spilloverRowIdIndexes.get(transactionId);
            if (index != null) {
                if (tryRemoveFromSpilledIndex(transaction, index, rowId)) {
                    return true;
                }
            }

            // Fall back to spill provider scan
            if (tryRemoveFromSpillProvider(transaction, rowId)) {
                return true;
            }
        }

        // Fallback to memory event store
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        return memoryEventStore.removeTransactionEventWithRowId(memTx, rowId);
    }

    private boolean tryRemoveFromSpilledIndex(T transaction, SpilloverRowIdIndex index, String rowId) {
        Deque<Integer> eventKeys = index.getRowIndex().get(rowId);
        if (eventKeys == null) {
            return false;
        }

        // Get the most recent eventKey for this rowId (last in the deque)
        // If eventKeys exists in the map, it must contain at least one element
        Integer eventKey = eventKeys.pollLast();

        // Remove the event from spillover cache
        boolean removed = spilloverCache.removeTransactionEventWithEventKey(transaction, eventKey);
        if (removed) {
            // Clean up the index if this was the last eventKey for this rowId
            if (eventKeys.isEmpty()) {
                index.getRowIndex().remove(rowId);
            }
        }
        return removed;
    }

    private boolean tryRemoveFromSpillProvider(T transaction, String rowId) {
        try {
            return spilloverCache.removeTransactionEventWithRowId(transaction, rowId);
        }
        catch (Exception e) {
            throw new DebeziumException(
                    "Fatal error while trying to remove rowId '" + rowId + "' from spillover storage for transaction " + transaction.getTransactionId(), e);
        }
    }

    @Override
    public boolean containsTransactionEvent(T transaction, int eventKey) {
        String transactionId = transaction.getTransactionId();

        // Check memory event store first
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        if (memoryEventStore.containsTransactionEvent(memTx, eventKey)) {
            return true;
        }

        // Check spillover if spilling
        if (isSpilling(transactionId)) {
            return spilloverCache.containsTransactionEvent(transaction, eventKey);
        }

        return false;
    }

    @Override
    public int getTransactionEventCount(T transaction) {
        String transactionId = transaction.getTransactionId();

        // Count from memory event store
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        int count = memoryEventStore.getTransactionEventCount(memTx);

        try {
            // Add spilled count if transaction is spilling
            if (isSpilling(transactionId)) {
                int spilled = spilloverCache.getTransactionEventCount(transaction);
                if (spilled > 0) {
                    count += spilled;
                }
            }
        }
        catch (Exception e) {
            LOGGER.debug("Failed to get spilled event count for tx={}: {}", transactionId, e.getMessage());
        }

        return count;
    }

    @Override
    public int getTransactionEvents() {
        // Calculate on-demand from both caches
        int memoryEvents = memoryEventStore.getTransactionEvents();
        int spilloverEvents = spilloverCache.getTransactionEvents();
        return memoryEvents + spilloverEvents;
    }

    @Override
    public void clear() {
        transactionRegistry.clear();
        transactionStates.clear();
        memoryEventStore.clear();
        spilloverCache.clear();
        spilloverRowIdIndexes.clear();
    }

    @Override
    public void syncTransaction(T transaction) {
        String transactionId = transaction.getTransactionId();

        // Sync memory event store
        MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
        memoryEventStore.syncTransaction(memTx);

        // If spilling, also sync spillover cache
        if (isSpilling(transactionId)) {
            spilloverCache.syncTransaction(transaction);
        }
    }

    /**
     * Implementation of TransactionView for spill strategy decisions.
     */
    private class TransactionViewImpl implements TransactionView {
        private final String transactionId;

        TransactionViewImpl(String transactionId) {
            this.transactionId = transactionId;
        }

        @Override
        public long memoryEventCount() {
            MemoryTransaction memTx = memoryEventStore.getTransaction(transactionId);
            return memoryEventStore.getTransactionEventCount(memTx);
        }

        @Override
        public long totalEventCount() {
            T transaction = transactionRegistry.get(transactionId);
            if (transaction == null) {
                return 0;
            }

            long memoryCount = memoryEventCount();
            long spilledCount = 0;
            if (isSpilling(transactionId)) {
                spilledCount = spilloverCache.getTransactionEventCount(transaction);
            }
            return memoryCount + spilledCount;
        }
    }

    enum TransactionState {
        MEMORY,
        SPILLING
    }
}
