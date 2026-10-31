/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.connector.oracle.logminer.events.CachedEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.RowIdCodec;
import io.debezium.connector.oracle.logminer.events.RsIdCodec;
import io.debezium.relational.TableId;

/**
 * The fields of a transaction's events that the partial rollback logic reads, kept on the heap in one
 * primitive array per field and indexed by the event's position in the transaction: the event type with the
 * undo flag, the row id and rs id in their packed forms, the table as a number from a {@link TableDictionary}
 * and the XML sequence. About 35 bytes per event, whichever buffer holds the event itself.
 * <p>
 * Positions are only ever appended or dropped from the tail, so an event's position never changes while its
 * transaction is open. The arrays double until a growth step reaches the configured cap and grow by the cap
 * from then on.
 *
 * @author Chris Cranford
 */
public final class EventMetadataColumns {

    public static final int INITIAL_CAPACITY = 8;

    private static final byte ROLLBACK_FLAG = (byte) 0x80;
    private static final long NULL_RS_ID = -1L;
    private static final long UNPACKED_RS_ID = -2L;
    private static final EventType[] EVENT_TYPES = EventType.values();

    private final TableDictionary tables;
    private final int growthMax;

    private int count;
    private byte[] type = new byte[INITIAL_CAPACITY];
    private long[] rsIdBlockSequence = new long[INITIAL_CAPACITY];
    private short[] rsIdOffset = new short[INITIAL_CAPACITY];
    private long[] rowIdHigh = new long[INITIAL_CAPACITY];
    private long[] rowIdLow = new long[INITIAL_CAPACITY];
    private int[] table = new int[INITIAL_CAPACITY];
    private int[] sequence = new int[INITIAL_CAPACITY];

    // Only an rs id that is not in the packed form is kept as a string, keyed by event position
    private Map<Integer, String> unpackedRsIds;

    /**
     * @param tables the dictionary that interns the events' tables, shared by the cache
     * @param growthMax the most positions the arrays grow by at a time
     */
    public EventMetadataColumns(TableDictionary tables, int growthMax) {
        this.tables = tables;
        this.growthMax = growthMax;
    }

    /**
     * The capacity to grow to for {@code needed} positions: doubling until the step reaches {@code growthMax},
     * then growing by {@code growthMax} each time.
     */
    public static int grownCapacity(int capacity, int needed, int growthMax) {
        return Math.max(needed, capacity + Math.min(capacity, growthMax));
    }

    /**
     * Appends an event's fields.
     *
     * @return the event's position
     */
    public int append(CachedEvent event) {
        if (count == type.length) {
            final int size = grownCapacity(type.length, count + 1, growthMax);
            type = Arrays.copyOf(type, size);
            rsIdBlockSequence = Arrays.copyOf(rsIdBlockSequence, size);
            rsIdOffset = Arrays.copyOf(rsIdOffset, size);
            rowIdHigh = Arrays.copyOf(rowIdHigh, size);
            rowIdLow = Arrays.copyOf(rowIdLow, size);
            table = Arrays.copyOf(table, size);
            sequence = Arrays.copyOf(sequence, size);
        }

        final int id = count;
        type[id] = (byte) (event.getEventType().ordinal() | (event.isRollback() ? ROLLBACK_FLAG : 0));

        final RowIdCodec.Packed rowId = event.getRowId();
        rowIdHigh[id] = rowId.high();
        rowIdLow[id] = rowId.low();

        storeRsId(id, event.getRsId());
        table[id] = tables.intern(event.getTableId());
        sequence[id] = Math.toIntExact(event.getTransactionSequence());

        count++;
        return id;
    }

    /**
     * @return the number of positions, which is the next event's position
     */
    public int count() {
        return count;
    }

    /**
     * @return the capacity of the arrays
     */
    public int capacity() {
        return type.length;
    }

    /**
     * Drops every position from {@code newCount} on.
     */
    public void truncateTo(int newCount) {
        if (newCount >= count) {
            return;
        }
        if (unpackedRsIds != null) {
            unpackedRsIds.keySet().removeIf(id -> id >= newCount);
        }
        count = newCount;
    }

    public EventType eventType(int id) {
        return EVENT_TYPES[type[id] & 0x7F];
    }

    public boolean isRollback(int id) {
        return (type[id] & ROLLBACK_FLAG) != 0;
    }

    public int table(int id) {
        return table[id];
    }

    public TableId tableId(int id) {
        return tables.get(table[id]);
    }

    public boolean sameRowId(int first, int second) {
        return rowIdHigh[first] == rowIdHigh[second] && rowIdLow[first] == rowIdLow[second];
    }

    public boolean hasEmptyRowId(int id) {
        return rowIdHigh[id] == RowIdCodec.EMPTY_ROW_ID.high() && rowIdLow[id] == RowIdCodec.EMPTY_ROW_ID.low();
    }

    public RowIdCodec.Packed rowId(int id) {
        return new RowIdCodec.Packed(rowIdHigh[id], rowIdLow[id]);
    }

    public String rowIdAsString(int id) {
        return RowIdCodec.decode(rowId(id));
    }

    public boolean sameRsId(int first, int second) {
        if (rsIdBlockSequence[first] == UNPACKED_RS_ID || rsIdBlockSequence[second] == UNPACKED_RS_ID) {
            return Objects.equals(rsId(first), rsId(second));
        }
        return rsIdBlockSequence[first] == rsIdBlockSequence[second] && rsIdOffset[first] == rsIdOffset[second];
    }

    public String rsId(int id) {
        final long blockSequence = rsIdBlockSequence[id];
        if (blockSequence == NULL_RS_ID) {
            return null;
        }
        if (blockSequence == UNPACKED_RS_ID) {
            return unpackedRsIds.get(id);
        }
        return RsIdCodec.decode(blockSequence, rsIdOffset[id]);
    }

    public long sequence(int id) {
        return sequence[id];
    }

    private void storeRsId(int id, String rsId) {
        final RsIdCodec.Packed packed = RsIdCodec.encode(rsId);
        if (packed != null) {
            rsIdBlockSequence[id] = packed.blockSequence();
            rsIdOffset[id] = packed.offset();
        }
        else {
            rsIdOffset[id] = 0;
            if (rsId == null) {
                rsIdBlockSequence[id] = NULL_RS_ID;
            }
            else {
                rsIdBlockSequence[id] = UNPACKED_RS_ID;
                if (unpackedRsIds == null) {
                    unpackedRsIds = new HashMap<>();
                }
                unpackedRsIds.put(id, rsId);
            }
        }
    }
}
