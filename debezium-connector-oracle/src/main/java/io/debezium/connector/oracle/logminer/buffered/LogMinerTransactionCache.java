/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A cache implementation that stores transactions and their respective events.
 *
 * Rather than use something like {@link LogMinerCache}, using a specialized implementation that combines
 * two separate but related objects allows for better overall management and handling that can be tailored
 * specifically to each implementation as needed.
 *
 * @author Chris Cranford
 */
public interface LogMinerTransactionCache<T extends Transaction> {
    /**
     * Get the transaction by transaction identifier.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     * @return the transaction instance if found, {@code null} if the lookup fails
     */
    T getTransaction(String transactionId);

    /**
     * Get the transaction by identifier and remove it from the cache if it exists.
     *
     * This method only removes the transaction from the transaction cache, but it does
     * remove the events associated with the transaction.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     * @return the transaction instance if found, {@code null} if the lookup fails
     */
    T getAndRemoveTransaction(String transactionId);

    /**
     * Adds the transaction to the cache.
     *
     * @param transaction the transaction to add, should not be {@code null}
     */
    void addTransaction(T transaction);

    /**
     * Removes the specified transaction from the cache.
     *
     * @param transaction the transaction to be removed, should not be {@code null}
     */
    void removeTransaction(T transaction);

    /**
     * Check whether the cache has a specific transaction by transaction identifier.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     * @return {@code true} if the transaction is cached, {@code false} if it is not cached
     */
    boolean containsTransaction(String transactionId);

    /**
     * Returns whether the transaction cache is empty.
     *
     * @return {@code true} if the cache is empty, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Returns the number of cached transactions.
     *
     * @return the number of transactions currently cached
     */
    int getTransactionCount();

    /**
     * Returns the eldest transaction scn details in the cache.
     *
     * @return the eldest transaction scn details, an empty optional if cache is empty
     */
    Optional<ScnDetails> getEldestTransactionScnDetailsInCache();

    /**
     * Consume all transactions as a {@link Stream} that exist in the cache and return a
     * computed value based on the iteration.
     *
     * @param consumer the transaction stream consumer, should not be {@code null}
     * @return the computed result of the stream consumer function
     */
    <R> R streamTransactionsAndReturn(Function<Stream<T>, R> consumer);

    /**
     * Apply a consumer to all transactions that exist in the cache.
     *
     * @param consumer the transaction stream consumer, should not be {@code null}
     */
    void transactions(Consumer<Stream<T>> consumer);

    /**
     * Get the transaction event by transaction reference and event key.
     *
     * @param transaction the transaction, should not be {@code null}
     * @param eventKey the event key, should not be {@code null}
     * @return the event if found, {@code null} if not found
     */
    LogMinerEvent getTransactionEvent(T transaction, int eventKey);

    /**
     * Applies a consumer to all event keys in the cache.
     * No assumptions should be made about the order of the event keys.
     *
     * @param consumer the consumer to be applied, should not be {@code null}
     */
    void eventKeys(Consumer<Stream<String>> consumer);

    /**
     * Apply a predicate over all cached events associated with the specified transaction.
     * The events will be supplied in insertion order.
     *
     * @param transaction the transaction, should not be {@code null}
     * @param predicate the consumer, should not be {@code null}
     * @throws InterruptedException thrown if the thread is interrupted
     */
    void forEachEvent(T transaction, InterruptiblePredicate<LogMinerEvent> predicate) throws InterruptedException;

    /**
     * Add a transaction event to the cache.
     *
     * @param transaction the transaction, should not be {@code null}
     * @param eventKey the event key, should not be {@code null}
     * @param event the event, should not be {@code null}
     */
    void addTransactionEvent(T transaction, int eventKey, LogMinerEvent event);

    /**
     * Removes all events for a given transaction.
     *
     * @param transaction the transaction, should not be {@code null}
     */
    void removeTransactionEvents(T transaction);

    /**
     * Removes a specific transaction event by unique row identifier.
     *
     * @param transaction the transaction, should not be {@code null}
     * @param rowId the event's unique row identifier
     * @return {@code true} if the event was found and removed, {@code false} if it was not found
     */
    boolean removeTransactionEventWithRowId(T transaction, String rowId);

    /**
     * Checks whether a specific transaction's event with the event key is cached.
     *
     * @param transaction the transaction, should not be {@code null}
     * @param eventKey the event's unique assigned id
     * @return {@code true} if the event is found, {@code false} otherwise
     */
    boolean containsTransactionEvent(T transaction, int eventKey);

    /**
     * Get the number of events associated with a specific transaction.
     *
     * @param transaction the transaction, should not be {@code null}
     * @return the number of events for tha given transaction
     */
    int getTransactionEventCount(T transaction);

    /**
     * Get the total number of events cached irrespective of transaction.
     *
     * @return the total number of cached events
     */
    int getTransactionEvents();

    /**
     * Abandon the specific transaction. An abandoned transaction will have all its events
     * and transaction details purged from the cache; however, a record of the transaction
     * identifier will be retained until explicitly removed.
     *
     * @param transaction the transaction to abandon, should not be {@code null}
     */
    void abandon(T transaction);

    /**
     * Removes the specific transaction identifier from the abandoned transaction cache.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     */
    void removeAbandonedTransaction(String transactionId);

    /**
     * Check whether the specified transaction identifier is marked as abandoned.
     *
     * @param transactionId the transaction identifier, should not be {@code null}
     * @return {@code true} if the transaction is abandoned, {@code false} otherwise
     */
    boolean isAbandoned(String transactionId);

    /**
     * Clears the contents of the cache.
     */
    void clear();

    /**
     * Resets the transaction state as if it has no transaction events.
     *
     * @param transaction the transaction to reset, should not be {@code null}
     */
    void resetTransactionToStart(T transaction);

    /**
     * Synchronizes the transaction state in heap with disk caches.
     *
     * @param transaction the transaction to synchronize, should not be {@code null}
     */
    void syncTransaction(T transaction);

    /**
     * Provides details about a specific scn found in the cache.
     *
     * @param scn the Oracle system change number, should not be {@code null}
     * @param changeTime the system change time, should not be {@code null}
     */
    record ScnDetails(Scn scn, Instant changeTime) {
    }

    @FunctionalInterface
    interface InterruptiblePredicate<T> {
        boolean test(T t) throws InterruptedException;
    }
}
