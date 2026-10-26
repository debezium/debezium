/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.connector.oracle.logminer.buffered.AbstractCacheProvider.REDO_EVENT_FORMAT_MARK;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.CHANGE_TIME;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.SCN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;

/**
 * A retained buffer is read only when it carries the stored format mark. A buffer written by a version that
 * stored parsed events carries none and must be cleared on reopening, while a marked buffer must be kept.
 *
 * @param <T> the transaction type
 * @author Chris Cranford
 */
public abstract class AbstractStoredEventFormatTest<T extends Transaction> {

    private static final String TRANSACTION_ID = "0a000b0000c40000";

    @AfterEach
    void dropRetainedCaches() throws Exception {
        // Opened with drop-on-stop, the provider drops its caches, leaving nothing for the next test class
        provider(true).close();
        deleteRetainedState();
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldClearRetainedBufferWithoutFormatMark() throws Exception {
        try (CacheProvider<T> provider = provider(false)) {
            store(provider, LogMinerRedoEventFixtures.insert());
            provider.getProcessedTransactionsCache().put(TRANSACTION_ID, "100");
            // What a buffer retained by a version that stored parsed events looks like
            provider.getProcessedTransactionsCache().removeIf(entry -> REDO_EVENT_FORMAT_MARK.equals(entry.getKey()));
        }

        try (CacheProvider<T> provider = provider(false)) {
            assertThat(provider.getTransactionCache().isEmpty()).isTrue();
            assertThat(provider.getTransactionCache().getTransactionEvents()).isZero();
            // Only the format mark remains, and the connector never asks for it as a transaction
            assertThat(provider.getProcessedTransactionsCache().size()).isEqualTo(1);
            assertThat(provider.getProcessedTransactionsCache().containsKey(TRANSACTION_ID)).isFalse();
        }
    }

    @Test
    @FixFor("debezium/dbz#2943")
    public void shouldKeepRetainedBufferWithFormatMark() throws Exception {
        try (CacheProvider<T> provider = provider(false)) {
            store(provider, LogMinerRedoEventFixtures.insert());
            provider.getProcessedTransactionsCache().put(TRANSACTION_ID, "100");
        }

        try (CacheProvider<T> provider = provider(false)) {
            final T transaction = provider.getTransactionCache().getTransaction("1");
            assertThat(transaction).isNotNull();
            assertThat(events(provider, transaction)).containsExactly(LogMinerRedoEventFixtures.insert());
            assertThat(provider.getProcessedTransactionsCache().size()).isEqualTo(2);
            assertThat(provider.getProcessedTransactionsCache().containsKey(TRANSACTION_ID)).isTrue();
        }
    }

    private void store(CacheProvider<T> provider, LogMinerRedoEvent event) {
        final T transaction = getTransactionFactory().createTransaction("1", SCN, CHANGE_TIME, "DEBEZIUM", 1, "client");
        provider.getTransactionCache().addTransaction(transaction);
        provider.getTransactionCache().addTransactionEvent(transaction, transaction.getNextEventId(), event);
        provider.getTransactionCache().syncTransaction(transaction);
    }

    private List<LogMinerRedoEvent> events(CacheProvider<T> provider, T transaction) throws InterruptedException {
        final List<LogMinerRedoEvent> result = new ArrayList<>();
        provider.getTransactionCache().forEachEvent(transaction, result::add);
        return result;
    }

    protected abstract CacheProvider<T> provider(boolean dropOnStop);

    protected abstract TransactionFactory<T> getTransactionFactory();

    /**
     * Removes whatever the persistent store keeps after its caches were dropped.
     */
    protected void deleteRetainedState() throws Exception {
    }
}
