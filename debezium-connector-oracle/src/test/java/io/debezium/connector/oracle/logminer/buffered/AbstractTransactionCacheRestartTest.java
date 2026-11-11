/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

/**
 * Verifies that a persisted transaction buffer is correctly restored after a graceful restart.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTransactionCacheRestartTest<T extends Transaction> {

    private static final TableId TABLE = TableId.parse("db.schema.table");
    private static final Instant CHANGE_TIME = Instant.now();

    @TempDir
    protected Path dataDir;

    private CacheProvider<T> cacheProvider;

    @AfterEach
    void afterEach() throws Exception {
        closeProvider();
    }

    @Test
    @FixFor("debezium/dbz#3060")
    public void shouldRestoreTransactionWithoutEventsAfterRestart() throws Exception {
        openProvider();
        assertThat(cacheProvider.getTransactionCache().isEmpty()).isTrue();

        // START mined, no captured DML yet
        cacheProvider.getTransactionCache().addTransaction(createTransaction("0a000100aa000000"));

        // Graceful stop and restart over the same persisted buffer
        closeProvider();
        openProvider();

        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        assertThat(cache.containsTransaction("0a000100aa000000")).isTrue();
        assertThat(cache.getTransactionCount()).isEqualTo(1);
        assertThat(cache.isEmpty()).isFalse();
        assertThat(cache.getTransactionEvents()).isEqualTo(0);

        // Re-mining the START event, then the first captured DML
        final T transaction = cache.getTransaction("0a000100aa000000");
        assertThat(transaction).isNotNull();
        cache.resetTransactionToStart(transaction);

        final int eventId = transaction.getNextEventId();
        assertThat(cache.containsTransactionEvent(transaction, eventId)).isFalse();
        cache.addTransactionEvent(transaction, eventId, event("1"));

        assertThat(cache.containsTransactionEvent(transaction, eventId)).isTrue();
        assertThat(cache.getTransactionEventCount(transaction)).isEqualTo(1);
        assertThat(cache.getTransactionEvents()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#3060")
    public void shouldRestoreTransactionsWithAndWithoutEventsAfterRestart() throws Exception {
        openProvider();

        final T withoutEvents = createTransaction("0a000100aa000000");
        final T withEvents = createTransaction("0a000200bb000000");
        cacheProvider.getTransactionCache().addTransaction(withoutEvents);
        cacheProvider.getTransactionCache().addTransaction(withEvents);
        cacheProvider.getTransactionCache().addTransactionEvent(withEvents, withEvents.getNextEventId(), event("1"));
        cacheProvider.getTransactionCache().addTransactionEvent(withEvents, withEvents.getNextEventId(), event("2"));
        cacheProvider.getTransactionCache().syncTransaction(withEvents);

        closeProvider();
        openProvider();

        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        assertThat(cache.getTransactionCount()).isEqualTo(2);
        assertThat(cache.containsTransaction(withoutEvents.getTransactionId())).isTrue();
        assertThat(cache.containsTransaction(withEvents.getTransactionId())).isTrue();
        assertThat(cache.getTransactionEvents()).isEqualTo(2);

        final T restoredWithEvents = cache.getTransaction(withEvents.getTransactionId());
        assertThat(cache.getTransactionEventCount(restoredWithEvents)).isEqualTo(2);

        final T restoredWithoutEvents = cache.getTransaction(withoutEvents.getTransactionId());
        assertThat(cache.getTransactionEventCount(restoredWithoutEvents)).isEqualTo(0);
        cache.addTransactionEvent(restoredWithoutEvents, restoredWithoutEvents.getNextEventId(), event("3"));

        assertThat(cache.getTransactionEventCount(restoredWithoutEvents)).isEqualTo(1);
        assertThat(cache.getTransactionEvents()).isEqualTo(3);
    }

    protected abstract CacheProvider<T> createCacheProvider();

    protected abstract TransactionFactory<T> getTransactionFactory();

    private void openProvider() {
        cacheProvider = createCacheProvider();
    }

    private void closeProvider() throws Exception {
        if (cacheProvider != null) {
            cacheProvider.close();
            cacheProvider = null;
        }
    }

    private T createTransaction(String transactionId) {
        return getTransactionFactory().createTransaction(transactionId, Scn.ONE, CHANGE_TIME, "userName", 1, "clientId");
    }

    private static LogMinerEvent event(String rsId) {
        return new LogMinerEvent(EventType.INSERT, Scn.ONE, TABLE, "AAAAAAAAAAAAAAAAAA", rsId, CHANGE_TIME);
    }
}
