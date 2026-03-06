/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

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
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryBasedLogMinerCache;
import io.debezium.util.Strings;

import net.openhft.chronicle.queue.RollCycles;

/**
 * Chronicle Queue-based cache provider implementation that creates Chronicle-backed transaction cache.
 * This provider enables spill-to-disk functionality for LogMiner transactions.
 *
 * @author Debezium Team
 */
public class ChronicleCacheProvider extends AbstractCacheProvider<ChronicleTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleCacheProvider.class);

    private final CompositeTransactionCache<ChronicleTransaction> transactionCache;
    private final MemoryBasedLogMinerCache<String, String> processedTransactionsCache;
    private final MemoryBasedLogMinerCache<String, String> schemaChangesCache;

    public ChronicleCacheProvider(OracleConnectorConfig connectorConfig) {

        if (!connectorConfig.isLogMiningBufferSpillEnabled()) {
            throw new DebeziumException("Chronicle Queue spill must be enabled to use ChronicleCacheProvider");
        }

        String spillPath = connectorConfig.getLogMiningBufferSpillPath();
        if (Strings.isNullOrEmpty(spillPath)) {
            throw new DebeziumException("Chronicle Queue spill path must be configured when spill is enabled");
        }

        Path spillDirectory = Paths.get(spillPath);
        long spillThreshold = connectorConfig.getLogMiningBufferSpillThreshold();
        String rollCycle = connectorConfig.getChronicleRollCycle();

        if (Strings.isNullOrEmpty(rollCycle)) {
            throw new DebeziumException("Chronicle roll cycle must be configured when spill is enabled");
        }
        try {
            RollCycles.valueOf(rollCycle);
        }
        catch (IllegalArgumentException e) {
            throw new DebeziumException("Invalid Chronicle roll cycle name '" + rollCycle + "'", e);
        }

        LOGGER.info("Creating Chronicle Queue transaction cache with spill path: {} and threshold: {} events",
                spillDirectory, spillThreshold);
        LOGGER.info("Spillover data is ephemeral and will be cleaned up automatically on connector stop");

        // Proactively purge any leftover spill subdirectories from a previous unclean shutdown.
        purgeLeftoverSpillDirectories(spillDirectory);

        try {
            // Create the spillover cache and strategy
            ChronicleLogMinerTransactionCache spilloverCache = new ChronicleLogMinerTransactionCache(spillDirectory, rollCycle);
            SpillStrategy strategy = new EventCountStrategy(spillThreshold);

            // Create the composite transaction store with configured index threshold
            int indexThreshold = connectorConfig.getLogMiningBufferSpillInMemoryIndexThreshold();
            this.transactionCache = new CompositeTransactionCache<ChronicleTransaction>(spilloverCache, strategy, indexThreshold);

            // TODO: Investigate using Chronicle Map for disk-backed processedTransactions and schemaChanges caches
            this.processedTransactionsCache = new MemoryBasedLogMinerCache<>();
            this.schemaChangesCache = new MemoryBasedLogMinerCache<>();
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to create Chronicle Queue transaction cache", e);
        }
    }

    @Override
    public LogMinerTransactionCache<ChronicleTransaction> getTransactionCache() {
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
            // Always clean up spillover data on connector stop
            transactionCache.clear();
        }
    }

    private void purgeLeftoverSpillDirectories(Path spillDirectory) {
        if (spillDirectory == null) {
            return;
        }
        try (Stream<Path> children = Files.list(spillDirectory)) {
            children.filter(Files::isDirectory).forEach(dir -> {
                try {
                    LOGGER.info("Purging leftover Chronicle spill directory {} from previous unclean shutdown", dir);
                    deleteRecursively(dir);
                }
                catch (IOException e) {
                    LOGGER.warn("Failed to purge leftover Chronicle spill directory {}", dir, e);
                }
            });
        }
        catch (IOException e) {
            LOGGER.debug("No existing spill directories to purge at {} ({})", spillDirectory, e.getMessage());
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
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
