/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A concrete implementation of the {@link LogMinerTransactionCache} that stores transactions and events
 * in the JVM heap for fast access and management.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerTransactionCache extends AbstractLogMinerTransactionCache<MemoryTransaction> {

    private final Map<String, MemoryTransaction> transactionsByTransactionId = new HashMap<>();
    private final Map<String, List<LogMinerEventEntry>> eventsByTransactionId = new HashMap<>();
    private final Map<String, HashMap<Integer, LogMinerEvent>> eventsByEventIdByTransactionId = new HashMap<>();

    @Override
    public MemoryTransaction getTransaction(String transactionId) {
        return transactionsByTransactionId.get(transactionId);
    }

    @Override
    public void addTransaction(MemoryTransaction transaction) {
        transactionsByTransactionId.put(transaction.getTransactionId(), transaction);
    }

    @Override
    public void removeTransaction(MemoryTransaction transaction) {
        transactionsByTransactionId.remove(transaction.getTransactionId());
    }

    @Override
    public boolean containsTransaction(String transactionId) {
        return transactionsByTransactionId.containsKey(transactionId);
    }

    @Override
    public boolean isEmpty() {
        return transactionsByTransactionId.isEmpty();
    }

    @Override
    public int getTransactionCount() {
        return transactionsByTransactionId.size();
    }

    @Override
    public <R> R streamTransactionsAndReturn(Function<Stream<MemoryTransaction>, R> consumer) {
        return consumer.apply(transactionsByTransactionId.values().stream());
    }

    @Override
    public void transactions(Consumer<Stream<MemoryTransaction>> consumer) {
        consumer.accept(transactionsByTransactionId.values().stream());
    }

    @Override
    public void eventKeys(Consumer<Stream<String>> consumer) {
        consumer.accept(eventsByTransactionId.entrySet().stream()
                .flatMap(entry -> {
                    String outerKey = entry.getKey();
                    return entry.getValue().stream().map(LogMinerEventEntry::eventId).map(key -> outerKey + "-" + key);
                }));
    }

    @Override
    public void forEachEvent(MemoryTransaction transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException {
        final var events = eventsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            try (var stream = events.stream()) {
                final Iterator<LogMinerEventEntry> iterator = stream.iterator();
                while (iterator.hasNext()) {
                    if (!predicate.test(iterator.next().event)) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public LogMinerEvent getTransactionEvent(MemoryTransaction transaction, int eventKey) {
        final var eventsByEventId = eventsByEventIdByTransactionId.get(transaction.getTransactionId());
        if (eventsByEventId != null) {
            return eventsByEventId.get(eventKey);
        }
        return null;
    }

    @Override
    public MemoryTransaction getAndRemoveTransaction(String transactionId) {
        return transactionsByTransactionId.remove(transactionId);
    }

    @Override
    public void addTransactionEvent(MemoryTransaction transaction, int eventKey, LogMinerEvent event) {
        eventsByTransactionId.computeIfAbsent(transaction.getTransactionId(), (id) -> new ArrayList<>())
                .add(new LogMinerEventEntry(eventKey, event));
        eventsByEventIdByTransactionId.computeIfAbsent(transaction.getTransactionId(), (id) -> new HashMap<>())
                .put(eventKey, event);
    }

    @Override
    public void removeTransactionEvents(MemoryTransaction transaction) {
        eventsByTransactionId.remove(transaction.getTransactionId());
        eventsByEventIdByTransactionId.remove(transaction.getTransactionId());
    }

    @Override
    public boolean removeTransactionEventWithRowId(MemoryTransaction transaction, String rowId) {
        final var events = eventsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            for (int i = events.size() - 1; i >= 0; i--) {
                final LogMinerEventEntry entry = events.get(i);
                if (entry.event.getRowId().equals(rowId)) {
                    events.remove(i);
                    eventsByEventIdByTransactionId.get(transaction.getTransactionId()).remove(entry.eventId);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean containsTransactionEvent(MemoryTransaction transaction, int eventKey) {
        final var eventsByEventId = eventsByEventIdByTransactionId.get(transaction.getTransactionId());
        if (eventsByEventId != null) {
            return eventsByEventId.containsKey(eventKey);
        }
        return false;
    }

    @Override
    public int getTransactionEventCount(MemoryTransaction transaction) {
        final var events = eventsByTransactionId.get(transaction.getTransactionId());
        if (events != null) {
            return events.size();
        }
        return 0;
    }

    @Override
    public int getTransactionEvents() {
        return eventsByTransactionId.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public void clear() {
        transactionsByTransactionId.clear();
        eventsByTransactionId.clear();
        eventsByEventIdByTransactionId.clear();
    }

    @Override
    public void syncTransaction(MemoryTransaction transaction) {
        // Changing the heap instance is sufficient, therefore this is a no-op
    }

    /**
     * An event record used to map event-id and event since event-id is not currently stored
     * as part of the LogMinerEvent object. This enables fast cleanup during row-id removal.
     *
     * @param eventId the event's unique identifier
     * @param event the event object
     */
    record LogMinerEventEntry(int eventId, LogMinerEvent event) {
    }
}
