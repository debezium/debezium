/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * A wrapper around Chronicle Queue that encapsulates queue creation, configuration,
 * and basic append/read operations for the Oracle LogMiner spill-to-disk functionality.
 *
 * @author Debezium Authors
 */
public class ChronicleQueueWrapper implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueWrapper.class);

    private final String basePath;
    private final String transactionId;
    private final String rollCycleName;
    private ChronicleQueue queue;
    private ExcerptAppender appender;

    /**
     * Creates a new Chronicle Queue wrapper for the specified transaction.
     *
     * @param basePath the base path for Chronicle Queue storage
     * @param transactionId the transaction ID (used as subdirectory name)
     * @param rollCycleName the Chronicle Queue roll cycle name
     */
    public ChronicleQueueWrapper(String basePath, String transactionId, String rollCycleName) {
        this.basePath = basePath;
        this.transactionId = transactionId;
        this.rollCycleName = rollCycleName;
    }

    /**
     * Initializes the Chronicle Queue and creates the directory if needed.
     * Must be called before using append or read operations.
     */
    public synchronized void initialize() {
        if (queue != null) {
            return;
        }

        try {
            Path queuePath = Paths.get(basePath, transactionId);
            Files.createDirectories(queuePath);

            LOGGER.debug("Creating Chronicle Queue for transaction {} at path {} with roll cycle {}",
                    transactionId, queuePath, rollCycleName);

            RollCycles rollCycle = RollCycles.valueOf(rollCycleName);
            queue = SingleChronicleQueueBuilder.single(queuePath.toString())
                    .rollCycle(rollCycle)
                    .build();

            appender = queue.createAppender();

        }
        catch (IOException e) {
            throw new DebeziumException("Failed to create Chronicle Queue directory for transaction " + transactionId, e);
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to initialize Chronicle Queue for transaction " + transactionId, e);
        }
    }

    /**
     * Gets an appender for writing to the queue.
     * Must call initialize() first.
     *
     * @return the appender instance
     */
    public ExcerptAppender getAppender() {
        if (appender == null) {
            throw new IllegalStateException("ChronicleQueueWrapper not initialized for transaction " + transactionId);
        }
        return appender;
    }

    /**
     * Creates a new tailer for reading from the queue.
     * Must call initialize() first.
     *
     * @return a new tailer instance positioned at the start
     */
    public ExcerptTailer createTailer() {
        if (queue == null) {
            throw new IllegalStateException("ChronicleQueueWrapper not initialized for transaction " + transactionId);
        }
        return queue.createTailer();
    }

    public boolean isInitialized() {
        return queue != null;
    }

    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Deletes the queue directory and all its contents.
     * The queue must be closed before calling this method.
     */
    public void deleteQueueDirectory() {
        if (queue != null) {
            throw new IllegalStateException("Queue must be closed before deleting directory for transaction " + transactionId);
        }

        try {
            Path queuePath = Paths.get(basePath, transactionId);
            if (Files.exists(queuePath)) {
                LOGGER.debug("Deleting Chronicle Queue directory for transaction {} at path {}", transactionId, queuePath);
                deleteDirectoryRecursively(queuePath);
            }
        }
        catch (IOException e) {
            LOGGER.warn("Failed to delete Chronicle Queue directory for transaction {}", transactionId, e);
        }
    }

    @Override
    public synchronized void close() {
        if (appender != null) {
            try {
                appender.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing Chronicle Queue appender for transaction {}", transactionId, e);
            }
            appender = null;
        }

        if (queue != null) {
            try {
                queue.close();
            }
            catch (Exception e) {
                LOGGER.warn("Error closing Chronicle Queue for transaction {}", transactionId, e);
            }
            queue = null;
        }
    }

    private static void deleteDirectoryRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }

        try (Stream<Path> stream = Files.walk(path)) {
            List<Path> paths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path p : paths) {
                Files.deleteIfExists(p);
            }
        }
    }
}
