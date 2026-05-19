/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.chroniclequeue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.Strings;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

/**
 * Encapsulates the Chronicle Queue lifecycle: directory setup, queue creation, event serialization and
 * deserialization, cleanup, and more.  Used by both {@link ChronicleQueueProvider} and the
 * {@link HybridChronicleQueueProvider} to avoid duplicating disk IO plumbing.
 *
 * @author Chris Cranford
 */
class ChronicleQueueHelper implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleQueueHelper.class);

    private final ChronicleQueue chronicleQueue;
    private final ExcerptAppender appender;
    private final ExcerptTailer tailer;
    private final SourceRecordJsonSerializer serializer;
    private final Path queuePath;
    private final boolean ownsDirectory;

    ChronicleQueueHelper(String path, String tempDirPrefix) {
        if (Strings.isNullOrEmpty(path)) {
            try {
                this.queuePath = Files.createTempDirectory(tempDirPrefix);
                this.ownsDirectory = true;
            }
            catch (IOException e) {
                throw new DebeziumException("Failed to create temporary directory for Chronicle Queue", e);
            }
        }
        else {
            this.queuePath = Paths.get(path);
            this.ownsDirectory = false;
        }

        this.chronicleQueue = ChronicleQueue.singleBuilder(queuePath).build();
        this.appender = chronicleQueue.createAppender();
        this.tailer = chronicleQueue.createTailer();
        this.serializer = new SourceRecordJsonSerializer();
    }

    Path getQueuePath() {
        return queuePath;
    }

    boolean isTemporaryPath() {
        return ownsDirectory;
    }

    void write(DataChangeEvent event) {
        try (DocumentContext dc = appender.writingDocument()) {
            final Wire wire = dc.wire();
            if (wire == null) {
                throw new DebeziumException("Failed to acquire write context for Chronicle Queue");
            }
            serializer.write(event.getRecord(), wire);
        }
    }

    DataChangeEvent read() {
        try (DocumentContext dc = tailer.readingDocument()) {
            final Wire wire = dc.wire();
            if (!dc.isPresent() || wire == null) {
                return null;
            }
            final SourceRecord record = serializer.read(wire);
            return new DataChangeEvent(record);
        }
    }

    @Override
    public void close() {
        chronicleQueue.close();
        if (ownsDirectory) {
            deleteDirectory(queuePath);
        }
    }

    private void deleteDirectory(Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException e) {
            LOGGER.warn("Failed to delete Chronicle Queue directory: {}", path, e);
        }
    }
}