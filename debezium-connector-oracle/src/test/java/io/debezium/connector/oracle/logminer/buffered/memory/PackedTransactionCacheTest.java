/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.CHANGE_TIME;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.RS_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;

/**
 * Partial rollbacks resolved on read, and truncation of a transaction's packed store.
 *
 * @author Chris Cranford
 */
public class PackedTransactionCacheTest {

    private static final Scn ANCHOR = Scn.valueOf(100L);
    private static final String ROW_B = "BBBBBBBBBBBBBBBBBB";
    private static final String ROW_C = "CCCCCCCCCCCCCCCCCC";

    private final PackedTransactionCache cache = new PackedTransactionCache(256, 16, ChunkCompressor.lz4());

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldResolvePartialRollbackWhenRead() throws Exception {
        cache.append("1", ANCHOR, event(EventType.INSERT, false, ROW_B));
        cache.append("1", ANCHOR, event(EventType.INSERT, false, ROW_C));
        cache.append("1", ANCHOR, event(EventType.DELETE, true, ROW_C));

        // The undone insert keeps its position and still counts; the read leaves it out
        assertThat(cache.get("1").count()).isEqualTo(3);
        final List<LogMinerRedoEvent> events = read("1");
        assertThat(events).extracting(LogMinerRedoEvent::getRowIdAsString).containsExactly(ROW_B, ROW_C);
        assertThat(events).extracting(LogMinerRedoEvent::isRollback).containsExactly(false, true);
    }

    @Test
    @FixFor("debezium/dbz#2504")
    public void shouldDropEventsFromAPositionAndTheStoreWhenEmpty() throws Exception {
        cache.append("1", ANCHOR, event(EventType.INSERT, false, ROW_B));
        cache.append("1", ANCHOR, event(EventType.INSERT, false, ROW_C));
        cache.append("1", ANCHOR, event(EventType.DELETE, true, ROW_C));

        assertThat(cache.truncate("1", 2)).isEqualTo(1);
        assertThat(cache.count("1")).isEqualTo(2);
        // Nothing undoes the insert of row C any more
        assertThat(read("1")).extracting(LogMinerRedoEvent::getRowIdAsString).containsExactly(ROW_B, ROW_C);

        assertThat(cache.truncate("1", 2)).isZero();
        assertThat(cache.truncate("1", 0)).isEqualTo(2);
        assertThat(cache.get("1")).isNull();
        assertThat(cache.truncate("1", 0)).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldDropTablesWithTheEvents() {
        cache.append("1", ANCHOR, event(EventType.INSERT, false, ROW_B));
        assertThat(cache.tables().size()).isEqualTo(1);
        cache.clear();
        assertThat(cache.isEmpty()).isTrue();
        assertThat(cache.tables().size()).isZero();
    }

    private List<LogMinerRedoEvent> read(String transactionId) throws InterruptedException {
        final List<LogMinerRedoEvent> events = new ArrayList<>();
        cache.forEach(transactionId, events::add);
        return events;
    }

    private static LogMinerRedoEvent event(EventType eventType, boolean rollback, String rowId) {
        return new LogMinerRedoEvent(eventType, rollback, ANCHOR, TABLE_ID, rowId, RS_ID, CHANGE_TIME, rollback ? null : "sql", 0, null, 0L, 1L, 1L);
    }
}
