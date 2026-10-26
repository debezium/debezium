/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.CHANGE_TIME;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.ROW_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.RS_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.TABLE_ID;
import static io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures.assertSameEvent;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.LogMinerRedoEventFixtures;
import io.debezium.connector.oracle.logminer.buffered.TableDictionary;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

/**
 * The packed store on its own: appends, chunks, truncation, growth and skips.
 *
 * @author Chris Cranford
 */
public class PackedTransactionTest {

    private static final Scn ANCHOR = Scn.valueOf(8_800_000L);
    private static final int SMALL_CHUNK = 256;
    private static final int GROWTH_MAX = 16;

    private final TableDictionary tables = new TableDictionary();
    private final OraclePayloadCodec codec = new OraclePayloadCodec();

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldReadBackEveryShapeInOrder() throws Exception {
        final PackedTransaction transaction = transaction(ChunkCompressor.lz4());
        final List<LogMinerRedoEvent> expected = LogMinerRedoEventFixtures.allEvents();
        for (LogMinerRedoEvent event : expected) {
            transaction.append(event);
        }

        final List<LogMinerRedoEvent> actual = read(transaction);
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertSameEvent(expected.get(i), actual.get(i));
            assertSameEvent(expected.get(i), transaction.get(i));
        }
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldSealFullChunksAndKeepTheLastOpen() {
        final PackedTransaction transaction = transaction(ChunkCompressor.lz4());
        for (int i = 0; i < 12; i++) {
            transaction.append(insert(i));
        }

        // Each insert encodes to about 90 bytes, so 256 byte chunks hold two
        assertThat(transaction.chunkCount()).isGreaterThan(1);
        for (int chunk = 0; chunk < transaction.chunkCount() - 1; chunk++) {
            assertThat(transaction.isChunkSealed(chunk)).isTrue();
        }
        assertThat(transaction.isChunkSealed(transaction.chunkCount() - 1)).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldNotSealWithoutCompressor() {
        final PackedTransaction transaction = transaction(null);
        for (int i = 0; i < 12; i++) {
            transaction.append(insert(i));
        }

        assertThat(transaction.chunkCount()).isGreaterThan(1);
        for (int chunk = 0; chunk < transaction.chunkCount(); chunk++) {
            assertThat(transaction.isChunkSealed(chunk)).isFalse();
        }
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldGiveOversizedPayloadItsOwnChunk() throws Exception {
        final PackedTransaction transaction = transaction(ChunkCompressor.lz4());
        transaction.append(insert(0));
        final int before = transaction.chunkCount();
        transaction.append(LogMinerRedoEventFixtures.lobWriteWithLongRedo());
        transaction.append(insert(1));

        assertThat(transaction.chunkCount()).isEqualTo(before + 2);
        assertThat(transaction.isChunkSealed(before)).isTrue();
        final List<LogMinerRedoEvent> events = read(transaction);
        assertThat(events).hasSize(3);
        assertSameEvent(LogMinerRedoEventFixtures.lobWriteWithLongRedo(), events.get(1));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldTruncateIntoSealedChunkAndAppendAgain() throws Exception {
        final PackedTransaction transaction = transaction(ChunkCompressor.lz4());
        for (int i = 0; i < 12; i++) {
            transaction.append(insert(i));
        }
        assertThat(transaction.isChunkSealed(0)).isTrue();

        transaction.truncateTo(1);

        assertThat(transaction.count()).isEqualTo(1);
        assertThat(transaction.chunkCount()).isEqualTo(1);
        assertThat(transaction.isChunkSealed(0)).isFalse();

        transaction.append(insert(100));
        final List<LogMinerRedoEvent> events = read(transaction);
        assertThat(events).hasSize(2);
        assertSameEvent(insert(0), events.get(0));
        assertSameEvent(insert(100), events.get(1));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldTruncateToEmpty() throws Exception {
        final PackedTransaction transaction = transaction(ChunkCompressor.lz4());
        transaction.append(insert(0));
        transaction.truncateTo(0);

        assertThat(transaction.count()).isZero();
        assertThat(transaction.chunkCount()).isZero();
        assertThat(read(transaction)).isEmpty();

        transaction.append(insert(1));
        assertSameEvent(insert(1), read(transaction).get(0));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldDoubleUntilTheStepReachesTheCap() {
        final PackedTransaction transaction = transaction(null);
        assertThat(transaction.capacity()).isEqualTo(PackedTransaction.INITIAL_CAPACITY);

        appendUpTo(transaction, 9);
        assertThat(transaction.capacity()).isEqualTo(16);
        appendUpTo(transaction, 17);
        assertThat(transaction.capacity()).isEqualTo(32);
        // The step is now the cap, so growth is linear from here
        appendUpTo(transaction, 33);
        assertThat(transaction.capacity()).isEqualTo(48);
        appendUpTo(transaction, 49);
        assertThat(transaction.capacity()).isEqualTo(64);
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldLeaveOutTheEventsOfTheBitSet() throws Exception {
        final PackedTransaction transaction = transaction(ChunkCompressor.lz4());
        for (int i = 0; i < 12; i++) {
            transaction.append(insert(i));
        }
        final BitSet skipped = new BitSet();
        skipped.set(1, 3);
        skipped.set(11);

        // Skipping changes neither the count nor the positions
        assertThat(transaction.count()).isEqualTo(12);
        final List<LogMinerRedoEvent> events = new ArrayList<>();
        transaction.forEach(skipped, events::add);
        assertThat(events).hasSize(9);
        assertSameEvent(insert(0), events.get(0));
        assertSameEvent(insert(3), events.get(1));
        assertSameEvent(insert(10), events.get(8));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldStopWhenThePredicateReturnsFalse() throws Exception {
        final PackedTransaction transaction = transaction(null);
        for (int i = 0; i < 5; i++) {
            transaction.append(insert(i));
        }
        final List<LogMinerRedoEvent> events = new ArrayList<>();
        transaction.forEach(new BitSet(), event -> events.add(event) && events.size() < 2);
        assertThat(events).hasSize(2);
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldStoreScnsRelativeToTheAnchorInBothDirections() throws Exception {
        final PackedTransaction transaction = transaction(null);
        final LogMinerRedoEvent earlier = insert(0, Scn.valueOf(1L));
        final LogMinerRedoEvent later = insert(1, Scn.valueOf(Long.MAX_VALUE - 1));
        transaction.append(earlier);
        transaction.append(later);

        final List<LogMinerRedoEvent> events = read(transaction);
        assertSameEvent(earlier, events.get(0));
        assertSameEvent(later, events.get(1));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldStoreScnsBeyondTheLongRange() throws Exception {
        final Scn anchor = Scn.valueOf("18446744073709551000");
        final PackedTransaction transaction = new PackedTransaction(anchor, tables, codec, SMALL_CHUNK, GROWTH_MAX, null);
        final LogMinerRedoEvent before = insert(0, Scn.valueOf("18446744073709550990"));
        final LogMinerRedoEvent after = insert(1, Scn.valueOf("18446744073709551615"));
        transaction.append(before);
        transaction.append(after);

        final List<LogMinerRedoEvent> events = read(transaction);
        assertSameEvent(before, events.get(0));
        assertSameEvent(after, events.get(1));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldCrossTheLongBoundaryFromALongAnchor() throws Exception {
        final PackedTransaction transaction = new PackedTransaction(Scn.valueOf(Long.MAX_VALUE - 10), tables, codec, SMALL_CHUNK, GROWTH_MAX, null);
        final LogMinerRedoEvent event = insert(0, Scn.valueOf("9223372036854775817"));
        transaction.append(event);

        assertSameEvent(event, read(transaction).get(0));
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldKeepRsIdsThatAreNotPackedAndNullOnes() throws Exception {
        final PackedTransaction transaction = transaction(null);
        final LogMinerRedoEvent unpacked = event(EventType.INSERT, "1", TABLE_ID);
        final LogMinerRedoEvent nullRsId = event(EventType.INSERT, null, TABLE_ID);
        final LogMinerRedoEvent packed = event(EventType.INSERT, RS_ID, TABLE_ID);
        transaction.append(unpacked);
        transaction.append(nullRsId);
        transaction.append(packed);
        transaction.append(event(EventType.UPDATE, "1", TABLE_ID));

        final List<LogMinerRedoEvent> events = read(transaction);
        assertSameEvent(unpacked, events.get(0));
        assertSameEvent(nullRsId, events.get(1));
        assertSameEvent(packed, events.get(2));
        assertThat(transaction.metadata().sameRsId(0, 3)).isTrue();
        assertThat(transaction.metadata().sameRsId(0, 2)).isFalse();
        assertThat(transaction.metadata().sameRsId(1, 2)).isFalse();
        assertThat(transaction.metadata().sameRsId(2, 2)).isTrue();

        // Truncation drops the unpacked value with its event
        transaction.truncateTo(3);
        transaction.append(event(EventType.DELETE, RS_ID, TABLE_ID));
        assertThat(transaction.metadata().rsId(3)).isEqualTo(RS_ID);
    }

    @Test
    @FixFor("debezium/dbz#2935")
    public void shouldKeepNullTableAndInternTheOthers() throws Exception {
        final PackedTransaction transaction = transaction(null);
        final TableId other = TableId.parse("ORCLPDB1.DEBEZIUM.ORDERS");
        transaction.append(event(EventType.INTERNAL, RS_ID, null));
        transaction.append(event(EventType.INSERT, RS_ID, TABLE_ID));
        transaction.append(event(EventType.INSERT, RS_ID, other));
        transaction.append(event(EventType.INSERT, RS_ID, TableId.parse("ORCLPDB1.DEBEZIUM.CUSTOMERS")));

        assertThat(tables.size()).isEqualTo(2);
        assertThat(transaction.metadata().table(0)).isEqualTo(TableDictionary.NONE);
        assertThat(transaction.metadata().table(1)).isEqualTo(transaction.metadata().table(3));
        assertThat(transaction.metadata().table(1)).isNotEqualTo(transaction.metadata().table(2));

        final List<LogMinerRedoEvent> events = read(transaction);
        assertThat(events.get(0).getTableId()).isNull();
        assertThat(events.get(2).getTableId()).isEqualTo(other);
    }

    private PackedTransaction transaction(ChunkCompressor compressor) {
        return new PackedTransaction(ANCHOR, tables, codec, SMALL_CHUNK, GROWTH_MAX, compressor);
    }

    private static void appendUpTo(PackedTransaction transaction, int count) {
        while (transaction.count() < count) {
            transaction.append(insert(transaction.count()));
        }
    }

    private static List<LogMinerRedoEvent> read(PackedTransaction transaction) throws InterruptedException {
        final List<LogMinerRedoEvent> events = new ArrayList<>();
        transaction.forEach(new BitSet(), events::add);
        return events;
    }

    private static LogMinerRedoEvent insert(int number) {
        return insert(number, Scn.valueOf(ANCHOR.longValue() + number));
    }

    private static LogMinerRedoEvent insert(int number, Scn scn) {
        return new LogMinerRedoEvent(EventType.INSERT, false, scn, TABLE_ID, ROW_ID, RS_ID, CHANGE_TIME,
                "insert into \"DEBEZIUM\".\"CUSTOMERS\"(\"ID\",\"NAME\") values ('" + number + "','Kafka');", 0, null, 0L, 70_123L, 70_124L);
    }

    private static LogMinerRedoEvent event(EventType eventType, String rsId, TableId tableId) {
        return new LogMinerRedoEvent(eventType, false, ANCHOR, tableId, ROW_ID, rsId, CHANGE_TIME, null, 2, "Object information not available", 1L, -1L,
                70_124L);
    }
}
