/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.CHANGE_TIME;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.SCN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;

/**
 * Stores each sample event in a transaction cache and reads it back, checking every field.
 * <p>
 * Each cache implementation provides a subclass. Whether the read goes through the cache's serializer depends
 * on the implementation; see the subclass for what its configuration guarantees.
 *
 * @param <T> the transaction type
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerEventRoundTripTest<T extends Transaction> {

    private CacheProvider<T> cacheProvider;
    private TransactionFactory<T> transactionFactory;

    @BeforeEach
    void beforeEach() {
        cacheProvider = getCacheProvider();
        transactionFactory = getTransactionFactory();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (cacheProvider != null) {
            cacheProvider.close();
            cacheProvider = null;
        }
        transactionFactory = null;
    }

    @TestFactory
    public Stream<DynamicTest> shouldRoundTripEachEvent() {
        // The dynamic tests run after beforeEach and share its provider; each stores its sample in its own transaction
        final List<LogMinerRedoEvent> samples = sampleEvents();
        return Stream.iterate(0, i -> i + 1).limit(samples.size()).map(i -> DynamicTest.dynamicTest(sampleName(samples.get(i)), () -> {
            final LogMinerRedoEvent event = samples.get(i);
            final T transaction = addTransaction(String.valueOf(i + 1));
            addEvent(transaction, event);

            final List<LogMinerRedoEvent> events = events(transaction);
            assertThat(events).hasSize(1);
            assertSameEvent(event, events.get(0));
        }));
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldRoundTripAllEventsInOrder() throws Exception {
        final List<LogMinerRedoEvent> expected = sampleEvents();

        final T transaction = addTransaction("1");
        for (LogMinerRedoEvent event : expected) {
            addEvent(transaction, event);
        }

        final List<LogMinerRedoEvent> actual = events(transaction);
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertSameEvent(expected.get(i), actual.get(i));
        }
        assertThat(cacheProvider.getTransactionCache().getTransactionEventCount(transaction)).isEqualTo(expected.size());
    }

    private T addTransaction(String transactionId) {
        final T transaction = transactionFactory.createTransaction(transactionId, SCN, CHANGE_TIME, "DEBEZIUM", 1, "client");
        cacheProvider.getTransactionCache().addTransaction(transaction);
        return transaction;
    }

    private void addEvent(T transaction, LogMinerRedoEvent event) {
        cacheProvider.getTransactionCache().addTransactionEvent(transaction, transaction.getNextEventId(), event);
    }

    private List<LogMinerRedoEvent> events(T transaction) throws InterruptedException {
        final List<LogMinerRedoEvent> result = new ArrayList<>();
        cacheProvider.getTransactionCache().forEachEvent(transaction, result::add);
        return result;
    }

    protected abstract CacheProvider<T> getCacheProvider();

    protected abstract TransactionFactory<T> getTransactionFactory();

    /**
     * The events to store, one per shape the stored type can take; a rollback marker, if present, must come
     * first so that storing the list in order triggers no partial rollback.
     */
    protected abstract List<LogMinerRedoEvent> sampleEvents();

    /**
     * Compares every field of the stored type.
     */
    protected abstract void assertSameEvent(LogMinerRedoEvent expected, LogMinerRedoEvent actual);

    /**
     * The display name of a sample in the dynamic tests.
     */
    protected String sampleName(LogMinerRedoEvent event) {
        return event.getClass().getSimpleName() + " " + event.getEventType();
    }
}
