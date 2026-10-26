/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.EventMetadataColumns;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache.InterruptiblePredicate;
import io.debezium.connector.oracle.logminer.buffered.SavepointRollbacks;
import io.debezium.connector.oracle.logminer.buffered.TableDictionary;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;

/**
 * The events of one transaction in the heap buffer, stored without an object per event.
 * <p>
 * The fields that the partial rollback logic reads are the {@link EventMetadataColumns} every buffer keeps.
 * Next to them, also indexed by the event's position, this store keeps the SCN as a signed offset from the
 * transaction's start SCN and the change time. Everything else the event carries, which is mostly its redo
 * SQL, is encoded by {@link OraclePayloadCodec} into {@link PayloadChunk}s shared by consecutive events, and
 * three more arrays say where each event's payload lies.
 * <p>
 * Events are only ever appended, or dropped from the tail when a mining pass is discarded, so a position never
 * changes. A partial rollback is resolved when the transaction is read: {@link SavepointRollbacks} marks the
 * undone events in a bit set from the metadata alone, and {@link #forEach} rebuilds the full
 * {@link LogMinerRedoEvent} of every other event, so an undone event's payload is never read.
 *
 * @author Chris Cranford
 */
final class PackedTransaction {

    static final int INITIAL_CAPACITY = EventMetadataColumns.INITIAL_CAPACITY;

    private static final int INITIAL_CHUNK_SIZE = 1024;

    private final EventMetadataColumns metadata;
    private final OraclePayloadCodec codec;
    private final int chunkSize;
    private final ChunkCompressor compressor;

    // The SCN every event's SCN is stored relative to; the BigInteger form is only used when it does not fit a long
    private final long anchor;
    private final BigInteger anchorBig;

    private long[] scnDelta = new long[INITIAL_CAPACITY];
    private long[] changeTime = new long[INITIAL_CAPACITY];
    private int[] payloadChunk = new int[INITIAL_CAPACITY];
    private int[] payloadOffset = new int[INITIAL_CAPACITY];
    private int[] payloadLength = new int[INITIAL_CAPACITY];

    private final List<PayloadChunk> chunks = new ArrayList<>();

    /**
     * @param anchor the transaction's start SCN, which the event SCNs are stored relative to
     * @param tables the dictionary that interns the events' tables, shared by the cache
     * @param codec the payload encoder, shared by the cache
     * @param chunkSize the capacity of a payload chunk in bytes
     * @param growthMax the most slots the metadata arrays grow by at a time
     * @param compressor the compressor that seals full chunks, or {@code null} to keep them raw
     */
    PackedTransaction(Scn anchor, TableDictionary tables, OraclePayloadCodec codec, int chunkSize, int growthMax, ChunkCompressor compressor) {
        this.metadata = new EventMetadataColumns(tables, growthMax);
        this.codec = codec;
        this.chunkSize = chunkSize;
        this.compressor = compressor;

        final BigInteger start = anchor == null || anchor.isNull() ? BigInteger.ZERO : anchor.asBigInteger();
        if (start.bitLength() < 64) {
            this.anchor = start.longValue();
            this.anchorBig = null;
        }
        else {
            this.anchor = 0L;
            this.anchorBig = start;
        }
    }

    /**
     * Appends an event.
     *
     * @return the event's position
     */
    int append(LogMinerRedoEvent event) {
        final int id = metadata.append(event);
        if (metadata.capacity() > scnDelta.length) {
            // The metadata columns grew, so the arrays kept here follow to the same capacity
            final int size = metadata.capacity();
            scnDelta = Arrays.copyOf(scnDelta, size);
            changeTime = Arrays.copyOf(changeTime, size);
            payloadChunk = Arrays.copyOf(payloadChunk, size);
            payloadOffset = Arrays.copyOf(payloadOffset, size);
            payloadLength = Arrays.copyOf(payloadLength, size);
        }

        scnDelta[id] = packScn(event.getScn());
        changeTime[id] = event.getChangeTimeMillis();

        final int length = codec.encode(event);
        final PayloadChunk chunk = writableChunk(length);
        payloadChunk[id] = chunks.size() - 1;
        payloadOffset[id] = chunk.append(codec.buffer(), 0, length);
        payloadLength[id] = length;
        return id;
    }

    /**
     * @return the number of events appended, which is the next event's position
     */
    int count() {
        return metadata.count();
    }

    /**
     * @return the fields the partial rollback logic reads, which {@link SavepointRollbacks} resolves over
     */
    EventMetadataColumns metadata() {
        return metadata;
    }

    int chunkCount() {
        return chunks.size();
    }

    boolean isChunkSealed(int chunk) {
        return chunks.get(chunk).isSealed();
    }

    /**
     * Drops every event from {@code newCount} on, returning the chunks to the state they had before the
     * dropped events were appended.
     */
    void truncateTo(int newCount) {
        if (newCount >= count()) {
            return;
        }
        final int lastKept = newCount - 1;
        if (lastKept < 0) {
            chunks.clear();
        }
        else {
            final int keepChunk = payloadChunk[lastKept];
            chunks.subList(keepChunk + 1, chunks.size()).clear();
            chunks.get(keepChunk).resetTo(payloadOffset[lastKept] + payloadLength[lastKept]);
        }
        metadata.truncateTo(newCount);
    }

    Scn scn(int id) {
        final long delta = scnDelta[id];
        if (anchorBig == null) {
            final long value = anchor + delta;
            if (((anchor ^ value) & (delta ^ value)) >= 0) {
                return Scn.valueOf(value);
            }
            return new Scn(BigInteger.valueOf(anchor).add(BigInteger.valueOf(delta)));
        }
        return new Scn(anchorBig.add(BigInteger.valueOf(delta)));
    }

    /**
     * Rebuilds one event, reading its chunk on its own; reads of many events go through {@link #forEach}.
     */
    LogMinerRedoEvent get(int id) {
        return materialize(id, chunks.get(payloadChunk[id]).open());
    }

    /**
     * Rebuilds the events whose bit is clear in {@code skipped}, in order, until the predicate returns
     * {@code false}. A sealed chunk is restored once, when the first event it holds is reached.
     *
     * @param skipped the events to leave out, as {@link SavepointRollbacks#resolve} computes them
     */
    void forEach(BitSet skipped, InterruptiblePredicate<LogMinerRedoEvent> predicate) throws InterruptedException {
        int openChunk = -1;
        byte[] bytes = null;
        for (int id = skipped.nextClearBit(0); id < count(); id = skipped.nextClearBit(id + 1)) {
            if (payloadChunk[id] != openChunk) {
                openChunk = payloadChunk[id];
                bytes = chunks.get(openChunk).open();
            }
            if (!predicate.test(materialize(id, bytes))) {
                return;
            }
        }
    }

    private LogMinerRedoEvent materialize(int id, byte[] chunkBytes) {
        return OraclePayloadCodec.decode(chunkBytes, payloadOffset[id], payloadLength[id], metadata.eventType(id), metadata.isRollback(id), scn(id),
                metadata.tableId(id), metadata.rowId(id), metadata.rsId(id), changeTime[id], metadata.sequence(id));
    }

    private long packScn(Scn scn) {
        final BigInteger value = scn.asBigInteger();
        if (value == null) {
            throw new IllegalArgumentException("A stored event must have an SCN");
        }
        if (anchorBig == null && value.bitLength() < 64) {
            return Math.subtractExact(value.longValue(), anchor);
        }
        // Only reached for SCNs beyond 63 bits; longValueExact fails only when the span itself exceeds 63 bits
        return value.subtract(anchorBig == null ? BigInteger.valueOf(anchor) : anchorBig).longValueExact();
    }

    private PayloadChunk writableChunk(int bytes) {
        if (chunks.isEmpty() || !chunks.get(chunks.size() - 1).fits(bytes)) {
            if (!chunks.isEmpty()) {
                chunks.get(chunks.size() - 1).seal();
            }
            // A payload larger than a chunk gets a chunk of its own, sized to fit
            chunks.add(new PayloadChunk(Math.max(chunkSize, bytes), Math.max(INITIAL_CHUNK_SIZE, bytes), compressor));
        }
        return chunks.get(chunks.size() - 1);
    }

    /**
     * @return the capacity of the metadata arrays, for tests of the growth policy
     */
    int capacity() {
        return metadata.capacity();
    }
}
