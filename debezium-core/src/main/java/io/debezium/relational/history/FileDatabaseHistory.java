/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.util.Collect;
import io.debezium.util.FunctionalReadWriteLock;

/**
 * A {@link DatabaseHistory} implementation that stores the schema history in a local file.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public final class FileDatabaseHistory extends AbstractDatabaseHistory {

    public static final Field FILE_PATH = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "file.filename")
            .withDescription("The path to the file that will be used to record the database history")
            .required();

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(FILE_PATH);

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    private Path path;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new ConnectException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        config.validateAndRecord(ALL_FIELDS, logger::error);
        if (running.get()) {
            throw new IllegalStateException("Database history file already initialized to " + path);
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        path = Paths.get(config.getString(FILE_PATH));
    }

    @Override
    public void start() {
        super.start();
        lock.write(() -> {
            if (running.compareAndSet(false, true)) {
                // todo: move to initializeStorage()
                Path path = this.path;
                if (path == null) {
                    throw new IllegalStateException("FileDatabaseHistory must be configured before it is started");
                }
                try {
                    // Make sure the file exists ...
                    if (!storageExists()) {
                        // Create parent directories if we have to.
                        // Checking for existence of the parent directory explicitly, as createDirectories()
                        // will raise an exception (despite stating the contrary in its JavaDoc) if the parent
                        // exists but is a sym-linked directory
                        if (path.getParent() != null && !Files.exists(path.getParent())) {
                            Files.createDirectories(path.getParent());
                        }
                        try {
                            Files.createFile(path);
                        }
                        catch (FileAlreadyExistsException e) {
                            // do nothing
                        }
                    }
                }
                catch (IOException e) {
                    throw new DatabaseHistoryException("Unable to create history file at " + path + ": " + e.getMessage(), e);
                }
            }
        });
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (record == null) {
            return;
        }
        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }
            try {
                String line = writer.write(record.document());
                // Create a buffered writer to write all of the records, closing the file when there is an error or when
                // the thread is no longer supposed to run
                try (BufferedWriter historyWriter = Files.newBufferedWriter(path, StandardOpenOption.APPEND)) {
                    try {
                        historyWriter.append(line);
                        historyWriter.newLine();
                    }
                    catch (IOException e) {
                        logger.error("Failed to add record to history at {}: {}", path, record, e);
                        return;
                    }
                }
                catch (IOException e) {
                    throw new DatabaseHistoryException("Unable to create writer for history file " + path + ": " + e.getMessage(), e);
                }
            }
            catch (IOException e) {
                logger.error("Failed to convert record to string: {}", record, e);
            }
        });
    }

    @Override
    public void stop() {
        running.set(false);
        super.stop();
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            try {
                if (exists()) {
                    for (String line : Files.readAllLines(path, UTF8)) {
                        if (line != null && !line.isEmpty()) {
                            records.accept(new HistoryRecord(reader.read(line)));
                        }
                    }
                }
            }
            catch (IOException e) {
                logger.error("Failed to add recover records from history at {}", path, e);
            }
        });
    }

    @Override
    public boolean storageExists() {
        return Files.exists(path);
    }

    @Override
    public boolean exists() {
        return storageExists();
    }

    @Override
    public String toString() {
        return "file " + (path != null ? path : "(unstarted)");
    }
}
