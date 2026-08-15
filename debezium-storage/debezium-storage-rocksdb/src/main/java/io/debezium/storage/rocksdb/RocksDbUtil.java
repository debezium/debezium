/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb;

import java.io.IOException;
import java.nio.file.Path;

import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.IoUtil;

/**
 * Utility methods shared by the RocksDB-backed storage implementations in this module.
 *
 * @author Chris Cranford
 */
public final class RocksDbUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbUtil.class);

    private RocksDbUtil() {
    }

    /**
     * Creates the {@link Options} used to open a RocksDB database. The caller owns the returned
     * instance and is responsible for closing it.
     *
     * @return configured options instance
     */
    public static Options createOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
    }

    /**
     * Recursively deletes a RocksDB storage directory, logging a warning rather than failing if the
     * directory or parts of it cannot be removed.
     *
     * @param directory the storage directory to delete; may be null
     */
    public static void deleteDirectory(Path directory) {
        if (directory == null) {
            return;
        }
        try {
            IoUtil.delete(directory);
        }
        catch (IOException e) {
            LOGGER.warn("Failed to delete RocksDB storage directory: {}", directory, e);
        }
    }
}