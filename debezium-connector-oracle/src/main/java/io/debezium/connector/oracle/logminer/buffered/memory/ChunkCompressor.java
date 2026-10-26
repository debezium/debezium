/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compresses a {@link PayloadChunk} when it is sealed and restores it when it is read.
 *
 * @author Chris Cranford
 */
interface ChunkCompressor {

    Logger LOGGER = LoggerFactory.getLogger(ChunkCompressor.class);

    /**
     * @param data the chunk's buffer
     * @param length the number of bytes in use at the start of the buffer
     * @return the compressed bytes, sized exactly
     */
    byte[] compress(byte[] data, int length);

    /**
     * @param compressed the bytes {@link #compress} returned
     * @param length the number of bytes before compression
     * @return the restored bytes, sized exactly {@code length}
     */
    byte[] decompress(byte[] compressed, int length);

    /**
     * The LZ4 compressor, which Kafka clients bring along, or {@code null} with a warning when its classes
     * are not on the classpath, in which case chunks stay uncompressed.
     */
    static ChunkCompressor lz4() {
        try {
            return new Lz4ChunkCompressor();
        }
        catch (NoClassDefFoundError e) {
            LOGGER.warn("LZ4 is not on the classpath; the transaction buffer stores its payload chunks uncompressed.");
            return null;
        }
    }
}
