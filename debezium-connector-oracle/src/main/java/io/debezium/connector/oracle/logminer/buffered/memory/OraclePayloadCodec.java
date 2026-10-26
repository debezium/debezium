/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.connector.oracle.logminer.events.RowIdCodec;
import io.debezium.relational.TableId;

/**
 * Encodes what a {@link LogMinerRedoEvent} carries beyond the fields {@link PackedTransaction} keeps in its
 * metadata columns: the status, the object ids, the {@code INFO} column and the redo SQL. Integers are written
 * as zig-zag variable-length values and strings as a length prefix followed by UTF-8, where a prefix of zero
 * stands for {@code null}.
 * <p>
 * The encoder writes into one reusable buffer, which the caller copies into a chunk before the next event is
 * encoded. One instance serves one cache, on the thread that mines.
 *
 * @author Chris Cranford
 */
final class OraclePayloadCodec {

    private byte[] buffer = new byte[1024];
    private int position;

    /**
     * Encodes the event's payload into {@link #buffer()}.
     *
     * @return the number of bytes written
     */
    int encode(LogMinerRedoEvent event) {
        position = 0;
        writeVarLong(event.getStatus());
        writeVarLong(event.getObjectId());
        writeVarLong(event.getDataObjectId());
        writeString(event.getInfo());
        writeString(event.getRedoSql());
        return position;
    }

    /**
     * @return the buffer the last {@link #encode} wrote into; valid until the next call
     */
    byte[] buffer() {
        return buffer;
    }

    /**
     * Rebuilds an event from its metadata and its encoded payload.
     */
    static LogMinerRedoEvent decode(byte[] data, int offset, int length, EventType eventType, boolean rollback, Scn scn, TableId tableId,
                                    RowIdCodec.Packed rowId, String rsId, long changeTime, long transactionSequence) {
        final Reader reader = new Reader(data, offset, offset + length);
        final int status = (int) reader.readVarLong();
        final long objectId = reader.readVarLong();
        final long dataObjectId = reader.readVarLong();
        final String info = reader.readString();
        final String redoSql = reader.readString();
        return new LogMinerRedoEvent(eventType, rollback, scn, tableId, rowId, rsId, changeTime, redoSql, status, info,
                transactionSequence, objectId, dataObjectId);
    }

    private void writeString(String value) {
        if (value == null) {
            writeVarLong(0);
            return;
        }
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeVarLong(bytes.length + 1L);
        ensure(bytes.length);
        System.arraycopy(bytes, 0, buffer, position, bytes.length);
        position += bytes.length;
    }

    private void writeVarLong(long value) {
        long zigzag = (value << 1) ^ (value >> 63);
        ensure(10);
        while ((zigzag & ~0x7FL) != 0) {
            buffer[position++] = (byte) ((zigzag & 0x7F) | 0x80);
            zigzag >>>= 7;
        }
        buffer[position++] = (byte) zigzag;
    }

    private void ensure(int bytes) {
        if (position + bytes > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(position + bytes, buffer.length * 2));
        }
    }

    private static final class Reader {
        private final byte[] data;
        private final int end;
        private int position;

        Reader(byte[] data, int position, int end) {
            this.data = data;
            this.position = position;
            this.end = end;
        }

        long readVarLong() {
            long zigzag = 0;
            int shift = 0;
            while (true) {
                final byte b = data[position++];
                zigzag |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            return (zigzag >>> 1) ^ -(zigzag & 1);
        }

        String readString() {
            final long prefix = readVarLong();
            if (prefix == 0) {
                return null;
            }
            final int length = (int) (prefix - 1);
            if (position + length > end) {
                throw new IllegalStateException("Payload string runs past the end of the payload");
            }
            final String value = new String(data, position, length, StandardCharsets.UTF_8);
            position += length;
            return value;
        }
    }
}
