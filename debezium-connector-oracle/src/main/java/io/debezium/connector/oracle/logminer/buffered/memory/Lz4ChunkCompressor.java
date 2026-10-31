/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Seals payload chunks with the fast LZ4 compressor, in its pure Java form so that no native library is loaded
 * into the connector's process.
 *
 * @author Chris Cranford
 */
final class Lz4ChunkCompressor implements ChunkCompressor {

    private static final LZ4Factory FACTORY = LZ4Factory.fastestJavaInstance();

    private final LZ4Compressor compressor = FACTORY.fastCompressor();
    private final LZ4FastDecompressor decompressor = FACTORY.fastDecompressor();

    @Override
    public byte[] compress(byte[] data, int length) {
        return compressor.compress(data, 0, length);
    }

    @Override
    public byte[] decompress(byte[] compressed, int length) {
        return decompressor.decompress(compressed, length);
    }
}
