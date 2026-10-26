/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.util.Arrays;

/**
 * An append-only byte buffer that holds the encoded payloads of consecutive events of one transaction.
 * <p>
 * A chunk is open while events are appended to it and grows up to its capacity; once an event no longer
 * fits, the transaction seals it and starts another. Sealing compresses the bytes in use and drops the
 * buffer, so a sealed chunk costs only its compressed size until it is read at commit, or reopened when a
 * truncation lands inside it. Without a compressor a chunk is never sealed and keeps its buffer as it is.
 *
 * @author Chris Cranford
 */
final class PayloadChunk {

    private final int capacity;
    private final ChunkCompressor compressor;

    private byte[] data;
    private int length;
    private boolean sealed;

    /**
     * @param capacity the most bytes the chunk accepts
     * @param initialSize the size of the first buffer, grown as needed up to the capacity
     * @param compressor the compressor that seals the chunk, or {@code null} to keep every chunk raw
     */
    PayloadChunk(int capacity, int initialSize, ChunkCompressor compressor) {
        this.capacity = capacity;
        this.compressor = compressor;
        this.data = new byte[Math.min(capacity, initialSize)];
    }

    /**
     * @return whether {@code bytes} more bytes can be appended
     */
    boolean fits(int bytes) {
        return !sealed && length + bytes <= capacity;
    }

    /**
     * Copies bytes to the end of the chunk, which must {@link #fits fit}.
     *
     * @return the offset the bytes were written at
     */
    int append(byte[] source, int sourceOffset, int bytes) {
        final int needed = length + bytes;
        if (needed > data.length) {
            data = Arrays.copyOf(data, Math.min(capacity, Math.max(needed, data.length * 2)));
        }
        System.arraycopy(source, sourceOffset, data, length, bytes);
        final int offset = length;
        length = needed;
        return offset;
    }

    /**
     * Compresses the bytes in use and releases the buffer; a no-op without a compressor or when already sealed.
     */
    void seal() {
        if (compressor != null && !sealed) {
            data = compressor.compress(data, length);
            sealed = true;
        }
    }

    /**
     * Drops every byte from {@code newLength} on, reopening the chunk first when it was sealed.
     */
    void resetTo(int newLength) {
        if (sealed) {
            data = compressor.decompress(data, length);
            sealed = false;
        }
        length = newLength;
    }

    /**
     * @return the chunk's bytes, of which the first {@link #length()} are in use; the chunk's own buffer when
     *         it is open, or a restored copy when it is sealed
     */
    byte[] open() {
        return sealed ? compressor.decompress(data, length) : data;
    }

    int length() {
        return length;
    }

    boolean isSealed() {
        return sealed;
    }
}
