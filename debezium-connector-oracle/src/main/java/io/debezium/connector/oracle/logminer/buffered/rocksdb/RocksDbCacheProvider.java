/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.buffered.AbstractCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.CompositeTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.EventCountStrategy;
import io.debezium.connector.oracle.logminer.buffered.LogMinerCache;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.SpillStrategy;

/**
 * A cache provider that uses RocksDB for the transaction cache.
 *
 * @author Debezium Authors
 */
public class RocksDbCacheProvider extends AbstractCacheProvider<RocksDbTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbCacheProvider.class);

    private final CompositeTransactionCache<RocksDbTransaction> transactionCache;
    private final RocksDbLogMinerTransactionCache spilloverCache;
    private final RocksDbLogMinerCache<String, String> processedTransactionsCache;
    private final RocksDbLogMinerCache<String, String> schemaChangesCache;
    private final Path dbPath;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDbCacheProvider(OracleConnectorConfig connectorConfig) {
        if (!connectorConfig.isLogMiningBufferSpillEnabled()) {
            throw new DebeziumException("RocksDB spill-to-disk is not enabled.");
        }

        this.dbPath = Paths.get(connectorConfig.getLogMiningBufferSpillPath());
        long spillThreshold = connectorConfig.getLogMiningBufferSpillThreshold();

        LOGGER.info("Creating RocksDB transaction cache with spill path: {} and threshold: {} events",
                dbPath, spillThreshold);
        LOGGER.info("Spillover data is ephemeral and will be cleaned up automatically on connector stop");

        try {
            if (Files.exists(dbPath)) {
                Files.walkFileTree(dbPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.deleteIfExists(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            Files.createDirectories(dbPath);
        }
        catch (IOException e) {
            throw new DebeziumException("Could not create RocksDB directory: " + dbPath, e);
        }

        try {
            // Create RocksDB Options from config
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setMaxBackgroundJobs(connectorConfig.getRocksDbMaxBackgroundJobs())
                    .setWriteBufferSize(connectorConfig.getRocksDbWriteBufferSizeMb() * 1024 * 1024)
                    .setMaxWriteBufferNumber(connectorConfig.getRocksDbMaxWriteBufferNumber())
                    .setTargetFileSizeBase(connectorConfig.getRocksDbTargetFileSizeMb() * 1024 * 1024)
                    .setMaxBytesForLevelBase(connectorConfig.getRocksDbMaxBytesForLevelBaseMb() * 1024 * 1024);

            // Set compression types
            String compression = connectorConfig.getRocksDbCompression();
            if ("LZ4".equalsIgnoreCase(compression)) {
                options.setCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION);
            }
            else if ("SNAPPY".equalsIgnoreCase(compression)) {
                options.setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION);
            }
            else if ("ZSTD".equalsIgnoreCase(compression)) {
                options.setCompressionType(org.rocksdb.CompressionType.ZSTD_COMPRESSION);
            }
            else if ("NONE".equalsIgnoreCase(compression)) {
                options.setCompressionType(org.rocksdb.CompressionType.NO_COMPRESSION);
            }
            else {
                throw new DebeziumException("Unsupported RocksDB compression type: " + compression);
            }

            String bottommostCompression = connectorConfig.getRocksDbBottommostCompression();
            if ("LZ4".equalsIgnoreCase(bottommostCompression)) {
                options.setBottommostCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION);
            }
            else if ("SNAPPY".equalsIgnoreCase(bottommostCompression)) {
                options.setBottommostCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION);
            }
            else if ("ZSTD".equalsIgnoreCase(bottommostCompression)) {
                options.setBottommostCompressionType(org.rocksdb.CompressionType.ZSTD_COMPRESSION);
            }
            else if ("NONE".equalsIgnoreCase(bottommostCompression)) {
                options.setBottommostCompressionType(org.rocksdb.CompressionType.NO_COMPRESSION);
            }
            else {
                throw new DebeziumException("Unsupported RocksDB bottommost compression type: " + bottommostCompression);
            }

            DBOptions dbOptions = new DBOptions(options);

            // Create the spillover cache with configured options
            this.spilloverCache = new RocksDbLogMinerTransactionCache(dbPath.toString(), options, dbOptions, connectorConfig.getRocksDbBatchFlushThreshold());
            SpillStrategy strategy = new EventCountStrategy(spillThreshold);

            // Create the composite transaction store with configured index threshold
            int indexThreshold = connectorConfig.getLogMiningBufferSpillInMemoryIndexThreshold();
            this.transactionCache = new CompositeTransactionCache<RocksDbTransaction>(this.spilloverCache, strategy, indexThreshold);

            // Create column families for processedTransactions and schemaChanges caches
            ColumnFamilyHandle processedTransactionsCF = spilloverCache.createColumnFamily(PROCESSED_TRANSACTIONS_CACHE_NAME);
            ColumnFamilyHandle schemaChangesCF = spilloverCache.createColumnFamily(SCHEMA_CHANGES_CACHE_NAME);

            // Create RocksDB-backed caches for processedTransactions and schemaChanges
            this.processedTransactionsCache = new RocksDbLogMinerCache<>(
                    spilloverCache.getDb(),
                    processedTransactionsCF,
                    PROCESSED_TRANSACTIONS_CACHE_NAME);
            this.schemaChangesCache = new RocksDbLogMinerCache<>(
                    spilloverCache.getDb(),
                    schemaChangesCF,
                    SCHEMA_CHANGES_CACHE_NAME);
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to create RocksDB transaction cache", e);
        }
    }

    @Override
    public LogMinerTransactionCache<RocksDbTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    public LogMinerCache<String, String> getSchemaChangesCache() {
        return schemaChangesCache;
    }

    @Override
    public LogMinerCache<String, String> getProcessedTransactionsCache() {
        return processedTransactionsCache;
    }

    @Override
    public void close() throws Exception {
        if (transactionCache != null) {
            transactionCache.clear();
            spilloverCache.close();
            // Remove the RocksDB directory so spillover data is ephemeral across connector restarts
            try {
                if (Files.exists(dbPath)) {
                    Files.walkFileTree(dbPath, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            Files.deleteIfExists(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                            Files.deleteIfExists(dir);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
            }
            catch (IOException e) {
                LOGGER.warn("Failed to delete RocksDB spill directory {}: {}", dbPath, e.getMessage());
            }

            LOGGER.info("Cleaned up and removed RocksDB spill directories and closed database (spillover data is ephemeral)");
        }
    }
}