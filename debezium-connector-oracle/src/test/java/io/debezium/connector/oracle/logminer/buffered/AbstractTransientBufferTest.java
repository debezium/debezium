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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;

/**
 * The buffer is transient: whatever a persistent buffer still holds when the connector starts is cleared,
 * and the in-flight transactions are re-mined from the offsets instead.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTransientBufferTest<T extends Transaction> {

    private static final String TRANSACTION_ID = "0a000b0000c40000";

    @AfterEach
    void dropRetainedCaches() throws Exception {
        // Opened with drop-on-stop, the provider drops its caches, leaving nothing for the next test class
        provider(true).close();
        deleteRetainedState();
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldStartEmptyWhenAPreviousRunLeftStateBehind() throws Exception {
        try (CacheProvider<T> provider = provider(false)) {
            store(provider, LogMinerRedoEventFixtures.insert());
            provider.getProcessedTransactionsCache().put(TRANSACTION_ID, "100");
        }
        try (CacheProvider<T> provider = provider(false)) {
            assertThat(provider.getTransactionCache().isEmpty()).isTrue();
            assertThat(provider.getTransactionCache().getTransactionEvents()).isZero();
            assertThat(provider.getTransactionCache().getTransaction("1")).isNull();
            assertThat(provider.getProcessedTransactionsCache().size()).isZero();
            assertThat(provider.getProcessedTransactionsCache().containsKey(TRANSACTION_ID)).isFalse();
            provider.getTransactionCache().eventKeys(keys -> assertThat(keys.count()).isZero());
        }
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldStoreAndReadWithinARun() throws Exception {
        try (CacheProvider<T> provider = provider(false)) {
            final T transaction = store(provider, LogMinerRedoEventFixtures.insert());
            assertThat(events(provider, transaction)).containsExactly(LogMinerRedoEventFixtures.insert());
        }
    }

    private T store(CacheProvider<T> provider, LogMinerRedoEvent event) {
        final T transaction = getTransactionFactory().createTransaction("1", SCN, CHANGE_TIME, "DEBEZIUM", 1, "client");
        provider.getTransactionCache().addTransaction(transaction);
        provider.getTransactionCache().addTransactionEvent(transaction, transaction.getNextEventId(), event);
        provider.getTransactionCache().syncTransaction(transaction);
        return transaction;
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
