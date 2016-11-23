/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
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
                                               .withValidation(Field::isRequired);

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(FILE_PATH);

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private Path path;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator) {
        lock.write(() -> {
            if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
                throw new ConnectException(
                        "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
            }
            config.validateAndRecord(ALL_FIELDS, logger::error);
            super.configure(config,comparator);
            path = Paths.get(config.getString(FILE_PATH));
        });
    }

    @Override
    protected void storeRecord(HistoryRecord record) {
        lock.write(() -> {
            try {
                String line = writer.write(record.document());
                if (!Files.exists(path)) {
                    Files.createDirectories(path.getParent());
                    try {
                        Files.createFile(path);
                    } catch (FileAlreadyExistsException e) {
                        // do nothing
                    }
                }
                Files.write(path, Collect.arrayListOf(line), UTF8, StandardOpenOption.APPEND);
            } catch (IOException e) {
                logger.error("Failed to add record to history at {}: {}", path, record, e);
            }
        });
    }

    @Override
    protected void recoverRecords(Tables schema, DdlParser ddlParser, Consumer<HistoryRecord> records) {
        lock.write(() -> {
            try {
                if (Files.exists(path)) {
                    for (String line : Files.readAllLines(path)) {
                        records.accept(new HistoryRecord(reader.read(line)));
                    }
                }
            } catch (IOException e) {
                logger.error("Failed to add recover records from history at {}", path, e);
            }
        });
    }

    @Override
    public String toString() {
        return "file " + (path != null ? path : "(unstarted)");
    }
}
