/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

/**
 * Tests rewinding a buffered transaction so that the events a mining step re-reads are buffered again.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTransactionCacheRewindTest<T extends Transaction> {

    private static final TableId TABLE = TableId.parse("db.schema.table");
    private static final Instant CHANGE_TIME = Instant.now();
    private static final String TRANSACTION_ID = "0102030405060708";

    private CacheProvider<T> cacheProvider;
    private TransactionFactory<T> transactionFactory;

    @BeforeEach
    void beforeEach() {
        cacheProvider = getCacheProvider(true);
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

    @Test
    @FixFor("debezium/dbz#3032")
    public void testRewoundEventsAreReplacedByReReadEvents() throws Exception {
        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        final T transaction = transaction();
        addEvents(cache, transaction, 101, 102, 103);

        cache.rewindTransaction(transaction, 1);
        assertThat(transaction.getNumberOfEvents()).isEqualTo(1);
        assertThat(cache.getTransactionEventCount(transaction)).isEqualTo(1);
        assertThat(cache.getTransactionEvents()).isEqualTo(1);
        assertThat(eventScns(cache, transaction)).containsExactly(101L);

        // The re-read returns a late row before the rewound events
        addEvents(cache, transaction, 150, 102, 103);
        assertThat(cache.getTransactionEventCount(transaction)).isEqualTo(4);
        assertThat(cache.getTransactionEvents()).isEqualTo(4);
        assertThat(eventScns(cache, transaction)).containsExactly(101L, 150L, 102L, 103L);
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testEventsBeforeRewindPointAreStillSkippedWhenReplayed() throws Exception {
        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        final T transaction = transaction();
        addEvents(cache, transaction, 101, 102, 103, 104);

        cache.rewindTransaction(transaction, 2);
        assertThat(cache.containsTransactionEvent(transaction, 1)).isTrue();
        assertThat(cache.containsTransactionEvent(transaction, 2)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testRewoundEventsNotReReadAreRemovedWithTransaction() throws Exception {
        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        final T transaction = transaction();
        addEvents(cache, transaction, 101, 102, 103);

        cache.rewindTransaction(transaction, 0);
        addEvents(cache, transaction, 101);
        assertThat(eventScns(cache, transaction)).containsExactly(101L);

        cache.removeTransactionEvents(transaction);
        assertThat(cache.getTransactionEvents()).isZero();
        assertThat(eventKeyCount(cache)).isZero();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testResetTransactionToStartAfterRewindStillSkipsBufferedEvents() throws Exception {
        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        final T transaction = transaction();
        addEvents(cache, transaction, 101, 102, 103);

        cache.rewindTransaction(transaction, 2);
        cache.resetTransactionToStart(transaction);
        assertThat(cache.containsTransactionEvent(transaction, transaction.getNextEventId())).isTrue();
        assertThat(cache.containsTransactionEvent(transaction, transaction.getNextEventId())).isTrue();
        assertThat(cache.containsTransactionEvent(transaction, transaction.getNextEventId())).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#3032")
    public void testTransactionWithAllEventsRewoundIsRestoredAfterRestart() throws Exception {
        Assumptions.assumeTrue(isBufferPersisted(), "The buffer is not persisted across restarts");

        // Restart with a buffer that is kept on stop
        cacheProvider.close();
        cacheProvider = getCacheProvider(false);
        final T transaction = transaction();
        addEvents(cacheProvider.getTransactionCache(), transaction, 101);
        cacheProvider.getTransactionCache().rewindTransaction(transaction, 0);
        cacheProvider.close();

        cacheProvider = getCacheProvider(true);
        final LogMinerTransactionCache<T> cache = cacheProvider.getTransactionCache();
        assertThat(cache.containsTransaction(TRANSACTION_ID)).isTrue();
        assertThat(cache.getTransactionEvents()).isZero();
        assertThat(eventKeyCount(cache)).isZero();

        final T restored = cache.getTransaction(TRANSACTION_ID);
        cache.resetTransactionToStart(restored);
        addEvents(cache, restored, 101);
        assertThat(eventScns(cache, restored)).containsExactly(101L);
    }

    private T transaction() {
        final T transaction = transactionFactory.createTransaction(TRANSACTION_ID, Scn.valueOf(100), CHANGE_TIME, "userName", 1, "clientId");
        cacheProvider.getTransactionCache().addTransaction(transaction);
        return transaction;
    }

    private static <T extends Transaction> void addEvents(LogMinerTransactionCache<T> cache, T transaction, long... scns) {
        for (long scn : scns) {
            final LogMinerEvent event = new LogMinerEvent(EventType.INSERT, Scn.valueOf(scn), TABLE, "AAAAAAAAAAAAAAAAAB", "0x01", CHANGE_TIME);
            cache.addTransactionEvent(transaction, transaction.getNextEventId(), event);
        }
    }

    private static <T extends Transaction> List<Long> eventScns(LogMinerTransactionCache<T> cache, T transaction) throws InterruptedException {
        final List<Long> scns = new ArrayList<>();
        cache.forEachEvent(transaction, event -> scns.add(event.getScn().longValue()));
        return scns;
    }

    private static <T extends Transaction> long eventKeyCount(LogMinerTransactionCache<T> cache) {
        final AtomicLong count = new AtomicLong();
        cache.eventKeys(stream -> count.set(stream.count()));
        return count.get();
    }

    protected abstract CacheProvider<T> getCacheProvider(boolean dropOnStop);

    protected boolean isBufferPersisted() {
        return false;
    }

    protected abstract TransactionFactory<T> getTransactionFactory();
}
