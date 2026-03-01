/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;

/**
 * Chronicle Queue-based implementation for transaction caching.
 * Handles the mechanics of spilling events to Chronicle Queue files.
 *
 * @author Debezium Authors
 */
public class ChronicleLogMinerTransactionCache extends AbstractLogMinerTransactionCache<ChronicleTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleLogMinerTransactionCache.class);

    // Single-threaded data structures (LogMiner processing is single-threaded)
    private final Map<String, ChronicleQueueWrapper> chronicleQueueByTx = new HashMap<>();
    private final Map<String, Map<Integer, Long>> chronicleIndexByTx = new HashMap<>();
    private final Map<String, TreeSet<Integer>> spilledEventKeyTombstonesByTx = new HashMap<>();
    private final Map<String, TreeSet<Integer>> eventIdsByTransactionId = new HashMap<>();
    private final Map<String, ChronicleTransaction> transactions = new HashMap<>();

    private final Path spillDirectory;
    private final String rollCycle;

    /**
     * Creates a new Chronicle spillover cache.
     *
     * @param spillDirectory the directory for Chronicle Queue storage
     * @param rollCycle the Chronicle Queue roll cycle
     */
    public ChronicleLogMinerTransactionCache(Path spillDirectory, String rollCycle) {
        this.spillDirectory = spillDirectory;
        this.rollCycle = rollCycle;
    }

    @Override
    public ChronicleTransaction getTransaction(String transactionId) {
        return transactions.get(transactionId);
    }

    @Override
    public void addTransaction(ChronicleTransaction transaction) {
        String txId = transaction.getTransactionId();
        transactions.put(txId, transaction);
        eventIdsByTransactionId.put(txId, new TreeSet<>());
    }

    @Override
    public void removeTransaction(ChronicleTransaction transaction) {
        String txId = transaction.getTransactionId();
        transactions.remove(txId);
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return eventIdsByTransactionId.containsKey(transactionId);
    }

    @Override
    public boolean isEmpty() {
        return eventIdsByTransactionId.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return eventIdsByTransactionId.size();
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<ChronicleTransaction>, R> consumer) {
        return consumer.apply(transactions.values().stream());
    }

    @Override
    public void transactions(Consumer<Stream<ChronicleTransaction>> consumer) {
        consumer.accept(transactions.values().stream());
    }

    @Override
    public void eventKeys(Consumer<Stream<String>> consumer) {
        consumer.accept(eventIdsByTransactionId.entrySet().stream()
                .flatMap(entry -> {
                    String transactionId = entry.getKey();
                    return entry.getValue().stream().map(eventId -> transactionId + "-" + eventId);
                }));
    }

    @Override
    public void forEachEvent(ChronicleTransaction transaction,
                             io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache.InterruptiblePredicate<LogMinerEvent> predicate)
            throws InterruptedException {
        String txId = transaction.getTransactionId();
        ChronicleQueueWrapper queueWrapper = chronicleQueueByTx.get(txId);
        if (queueWrapper == null || !queueWrapper.isInitialized()) {
            return;
        }

        TreeSet<Integer> tombstones = spilledEventKeyTombstonesByTx.get(txId);
        if (tombstones == null) {
            try (ExcerptTailer tailer = queueWrapper.createTailer()) {
                ChronicleEventSerializer.EventKeyAndEvent keyAndEvent;
                while ((keyAndEvent = ChronicleEventSerializer.readEventWithKey(tailer)) != null) {
                    if (keyAndEvent.event() instanceof DmlEvent) {
                        // event contains DML
                    }
                    if (!predicate.test(keyAndEvent.event())) {
                        break;
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Failed to iterate spilled events for transaction {}", txId, e);
            }
            return;
        }

        try (ExcerptTailer tailer = queueWrapper.createTailer()) {
            ChronicleEventSerializer.EventKeyAndEvent keyAndEvent;
            while ((keyAndEvent = ChronicleEventSerializer.readEventWithKey(tailer)) != null) {
                if (tombstones.contains(keyAndEvent.eventKey())) {
                    continue;
                }
                if (!predicate.test(keyAndEvent.event())) {
                    break;
                }
            }
        }
        catch (Exception e) {
            LOGGER.warn("Failed to iterate spilled events for transaction {}", txId, e);
        }
    }

    @Override
    public LogMinerEvent getTransactionEvent(ChronicleTransaction transaction, int eventKey) {
        String txId = transaction.getTransactionId();
        Map<Integer, Long> indexMap = chronicleIndexByTx.get(txId);
        if (indexMap != null) {
            Long index = indexMap.get(eventKey);
            if (index != null) {
                ChronicleQueueWrapper queueWrapper = chronicleQueueByTx.get(txId);
                if (queueWrapper != null && queueWrapper.isInitialized()) {
                    try (ExcerptTailer tailer = queueWrapper.createTailer()) {
                        if (tailer.moveToIndex(index)) {
                            ChronicleEventSerializer.EventKeyAndEvent keyAndEvent = ChronicleEventSerializer.readEventWithKey(tailer);
                            if (keyAndEvent != null && keyAndEvent.eventKey() == eventKey) {
                                // If this event was tombstoned, consider it removed.
                                TreeSet<Integer> tombstones = spilledEventKeyTombstonesByTx.get(txId);
                                if (tombstones != null && tombstones.contains(eventKey)) {
                                    return null;
                                }
                                return keyAndEvent.event();
                            }
                        }
                    }
                    catch (Exception e) {
                        LOGGER.warn("Failed to read event {} for transaction {} from index", eventKey, txId, e);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public ChronicleTransaction getAndRemoveTransaction(String transactionId) {
        ChronicleTransaction transaction = getTransaction(transactionId);
        if (transaction != null) {
            removeTransaction(transaction);
        }
        return transaction;
    }

    @Override
    public void addTransactionEvent(ChronicleTransaction transaction, int eventKey, LogMinerEvent event) {
        String txId = transaction.getTransactionId();
        TreeSet<Integer> eventSet = eventIdsByTransactionId.get(txId);
        if (eventSet == null) {
            throw new IllegalStateException("Transaction " + txId + " not initialized. Call addTransaction first.");
        }
        spillEventToChronicleQueue(txId, eventKey, event);
        eventSet.add(eventKey);
    }

    @Override
    public void removeTransactionEvents(ChronicleTransaction transaction) {
        String txId = transaction.getTransactionId();
        // Remove all event-related data structures (matching canonical pattern)
        eventIdsByTransactionId.remove(txId);
        chronicleIndexByTx.remove(txId);
        spilledEventKeyTombstonesByTx.remove(txId);

        // Close and clean up Chronicle queue resources
        ChronicleQueueWrapper wrapper = chronicleQueueByTx.remove(txId);
        if (wrapper != null) {
            try {
                wrapper.close();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to close Chronicle queue for transaction {}", txId, e);
            }
            try {
                wrapper.deleteQueueDirectory();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to delete Chronicle queue directory for transaction {}", txId, e);
            }
        }
    }

    @Override
    public boolean removeTransactionEventWithRowId(ChronicleTransaction transaction, String rowId) {
        return removeTransactionEventWithRowId(transaction.getTransactionId(), rowId);
    }

    @Override
    public boolean containsTransactionEvent(ChronicleTransaction transaction, int eventKey) {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events == null || !events.contains(eventKey)) {
            return false;
        }

        final var tombstones = spilledEventKeyTombstonesByTx.get(transaction.getTransactionId());
        return tombstones == null || !tombstones.contains(eventKey);
    }

    @Override
    public boolean removeTransactionEventWithEventKey(ChronicleTransaction transaction, int eventKey) {
        String txId = transaction.getTransactionId();
        TreeSet<Integer> tombstoneSet = spilledEventKeyTombstonesByTx.computeIfAbsent(txId, id -> new TreeSet<>());
        return tombstoneSet.add(eventKey);
    }

    @Override
    public int getTransactionEventCount(ChronicleTransaction transaction) {
        final var events = eventIdsByTransactionId.get(transaction.getTransactionId());
        if (events == null) {
            return 0;
        }

        final var tombstones = spilledEventKeyTombstonesByTx.get(transaction.getTransactionId());
        final int tombstoneCount = (tombstones != null) ? tombstones.size() : 0;

        return events.size() - tombstoneCount;
    }

    @Override
    public int getTransactionEvents() {
        long totalEvents = eventIdsByTransactionId.values().stream().mapToLong(Set::size).sum();
        long totalTombstones = spilledEventKeyTombstonesByTx.values().stream().mapToLong(Set::size).sum();
        return (int) (totalEvents - totalTombstones);
    }

    @Override
    public void clear() {
        transactions.clear();
        eventIdsByTransactionId.clear();
        chronicleIndexByTx.clear();
        spilledEventKeyTombstonesByTx.clear();

        chronicleQueueByTx.values().forEach(wrapper -> {
            try {
                wrapper.close();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to close Chronicle queue during clear", e);
            }
            try {
                wrapper.deleteQueueDirectory();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to delete Chronicle queue directory during clear", e);
            }
        });
        chronicleQueueByTx.clear();
    }

    @Override
    public void syncTransaction(ChronicleTransaction transaction) {
        // No-op
    }

    public boolean removeTransactionEventWithRowId(String txId, String rowId) {
        Integer latestEventKey = findLatestSpilledEventKey(txId, rowId);
        if (latestEventKey != null) {
            TreeSet<Integer> tombstoneSet = spilledEventKeyTombstonesByTx.computeIfAbsent(txId, id -> new TreeSet<>());
            tombstoneSet.add(latestEventKey);
            return true;
        }
        return false;
    }

    private void spillEventToChronicleQueue(String txId, int eventKey, LogMinerEvent event) {
        ChronicleQueueWrapper queueWrapper = chronicleQueueByTx.computeIfAbsent(txId,
                id -> {
                    ChronicleQueueWrapper wrapper = new ChronicleQueueWrapper(spillDirectory.toString(), id, rollCycle);
                    wrapper.initialize();
                    return wrapper;
                });
        try {
            long chronicleIndex = ChronicleEventSerializer.writeEvent(queueWrapper.getAppender(), eventKey, event);
            chronicleIndexByTx.computeIfAbsent(txId, k -> new HashMap<>()).put(eventKey, chronicleIndex);
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to write event " + eventKey + " to Chronicle Queue", e);
        }
    }

    /**
     * Finds the eventKey of the most recent spilled event with the specified rowId.
     * Iterates backward through spilled events to find the latest (most recent) event with matching rowId.
     *
     * @param txId the transaction ID
     * @param rowId the row ID to search for
     * @return the eventKey of the latest matching event, or null if not found
     */
    private Integer findLatestSpilledEventKey(String txId, String rowId) {
        ChronicleQueueWrapper queueWrapper = chronicleQueueByTx.get(txId);
        if (queueWrapper == null || !queueWrapper.isInitialized()) {
            return null;
        }

        TreeSet<Integer> tombstones = spilledEventKeyTombstonesByTx.computeIfAbsent(txId, k -> new TreeSet<>());
        try (ExcerptTailer tailer = queueWrapper.createTailer()) {
            tailer.direction(TailerDirection.BACKWARD).toEnd();

            ChronicleEventSerializer.EventKeyAndEvent keyAndEvent;
            while ((keyAndEvent = ChronicleEventSerializer.readEventWithKey(tailer)) != null) {
                if (tombstones.contains(keyAndEvent.eventKey())) {
                    continue;
                }
                if (rowId.equals(keyAndEvent.event().getRowId())) {
                    return keyAndEvent.eventKey();
                }
            }
        }
        catch (Exception e) {
            LOGGER.warn("Failed to find latest spilled event with rowId {} in transaction {}", rowId, txId, e);
        }
        return null;
    }
}
