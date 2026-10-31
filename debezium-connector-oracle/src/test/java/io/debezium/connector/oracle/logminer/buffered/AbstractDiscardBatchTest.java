/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.CHANGE_TIME;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.RS_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;

/**
 * A mining pass that fails part-way, for example with {@code ORA-00310}, is discarded and mined again: the
 * events the failed pass added are dropped, nothing else changes, and the second pass rebuilds the same
 * events. Run against each cache implementation.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDiscardBatchTest<T extends Transaction> {

    private static final Scn SCN = Scn.valueOf(100L);
    private static final String ROW_B = "BBBBBBBBBBBBBBBBBB";
    private static final String ROW_C = "CCCCCCCCCCCCCCCCCC";

    private CacheProvider<T> cacheProvider;
    private LogMinerTransactionCache<T> cache;
    private TransactionFactory<T> transactionFactory;

    @BeforeEach
    void beforeEach() {
        cacheProvider = getCacheProvider();
        cache = cacheProvider.getTransactionCache();
        transactionFactory = getTransactionFactory();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (cacheProvider != null) {
            cacheProvider.close();
            cacheProvider = null;
        }
    }

    @Test
    @FixFor("debezium/dbz#2504")
    public void shouldDiscardWhatTheFailedPassAddedAndMineItAgain() throws Exception {
        // The state before the pass that fails
        final T first = transaction("1");
        final T second = transaction("2");
        add(first, event(EventType.INSERT, false, ROW_B));
        add(first, event(EventType.INSERT, false, ROW_C));
        add(second, event(EventType.INSERT, false, ROW_B));
        assertThat(cache.getTransactionEvents()).isEqualTo(3);

        cache.beginBatch();
        // The undo in the failed pass reverts the latest change, which is from before the pass
        add(first, event(EventType.DELETE, true, ROW_C));
        add(first, event(EventType.UPDATE, false, ROW_B));
        final T third = transaction("3");
        add(third, event(EventType.INSERT, false, ROW_B));
        assertThat(cache.getTransactionEventCount(first)).isEqualTo(4);
        assertThat(cache.getTransactionEvents()).isEqualTo(6);
        assertThat(rowIds(first)).containsExactly(ROW_B, ROW_C, ROW_B);

        cache.discardBatch();

        assertThat(cache.getTransactionEventCount(first)).isEqualTo(2);
        assertThat(cache.getTransactionEventCount(second)).isEqualTo(1);
        assertThat(cache.getTransactionEventCount(third)).isZero();
        assertThat(cache.getTransactionEvents()).isEqualTo(3);
        assertThat(cache.containsTransactionEvent(first, 1)).isTrue();
        assertThat(cache.containsTransactionEvent(first, 2)).isFalse();
        // Nothing undoes the insert of row C any more, so the read returns it again
        assertThat(rowIds(first)).containsExactly(ROW_B, ROW_C);

        // The pass is mined again and the same rows arrive at the positions they had, handed out by the
        // transactions' own counters, which the discard rewound; the read resolves the undo as before
        final T firstAgain = cache.getTransaction("1");
        final T thirdAgain = cache.getTransaction("3");
        assertThat(firstAgain.getNumberOfEvents()).isEqualTo(2);
        assertThat(thirdAgain.getNumberOfEvents()).isZero();

        cache.beginBatch();
        add(firstAgain, event(EventType.DELETE, true, ROW_C));
        add(firstAgain, event(EventType.UPDATE, false, ROW_B));
        add(thirdAgain, event(EventType.INSERT, false, ROW_B));
        cache.completeBatch();

        assertThat(cache.getTransactionEventCount(firstAgain)).isEqualTo(4);
        assertThat(cache.getTransactionEvents()).isEqualTo(6);
        assertThat(rowIds(firstAgain)).containsExactly(ROW_B, ROW_C, ROW_B);
        assertThat(rowIds(thirdAgain)).containsExactly(ROW_B);
    }

    @Test
    @FixFor("debezium/dbz#2504")
    public void shouldLeaveATransactionCommittedByTheFailedPassAlone() throws Exception {
        final T first = transaction("1");
        add(first, event(EventType.INSERT, false, ROW_B));

        cache.beginBatch();
        add(first, event(EventType.INSERT, false, ROW_C));
        // The pass commits the transaction before it fails: the transaction is read, removed and its events dropped
        final T committed = cache.getAndRemoveTransaction("1");
        cache.removeTransactionEvents(committed);
        assertThat(cache.getTransactionEvents()).isZero();

        cache.discardBatch();

        assertThat(cache.getTransaction("1")).isNull();
        assertThat(cache.getTransactionEvents()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2504")
    public void shouldKeepEverythingWhenThePassCompletes() throws Exception {
        final T first = transaction("1");
        cache.beginBatch();
        add(first, event(EventType.INSERT, false, ROW_B));
        cache.completeBatch();

        cache.discardBatch();

        assertThat(cache.getTransactionEventCount(first)).isEqualTo(1);
        assertThat(cache.getTransactionEvents()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2504")
    public void shouldNotMarkOutsideAPass() throws Exception {
        final T first = transaction("1");
        add(first, event(EventType.INSERT, false, ROW_B));

        cache.discardBatch();

        assertThat(cache.getTransactionEventCount(first)).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2504")
    public void shouldIgnoreAnEventAlreadyStoredAndRejectAGap() throws Exception {
        final T first = transaction("1");
        add(first, event(EventType.INSERT, false, ROW_B));

        add(first, 0, event(EventType.INSERT, false, ROW_C));
        assertThat(cache.getTransactionEventCount(first)).isEqualTo(1);
        assertThat(cache.getTransactionEvents()).isEqualTo(1);

        assertThatThrownBy(() -> add(first, 5, event(EventType.INSERT, false, ROW_C))).isInstanceOf(IllegalArgumentException.class);
    }

    private T transaction(String transactionId) {
        final T transaction = transactionFactory.createTransaction(transactionId, SCN, CHANGE_TIME, "DEBEZIUM", 1, "client");
        cache.addTransaction(transaction);
        return transaction;
    }

    private void add(T transaction, LogMinerRedoEvent event) {
        add(transaction, transaction.getNextEventId(), event);
    }

    private void add(T transaction, int eventKey, LogMinerRedoEvent event) {
        cache.addTransactionEvent(transaction, eventKey, event);
    }

    private List<String> rowIds(T transaction) throws InterruptedException {
        final List<String> rowIds = new ArrayList<>();
        cache.forEachEvent(transaction, event -> rowIds.add(event.getRowIdAsString()));
        return rowIds;
    }

    private static LogMinerRedoEvent event(EventType eventType, boolean rollback, String rowId) {
        return new LogMinerRedoEvent(eventType, rollback, SCN, TABLE_ID, rowId, RS_ID, CHANGE_TIME, rollback ? null : "sql", 0, null, 0L, 1L, 1L);
    }

    protected abstract CacheProvider<T> getCacheProvider();

    protected abstract TransactionFactory<T> getTransactionFactory();
}
