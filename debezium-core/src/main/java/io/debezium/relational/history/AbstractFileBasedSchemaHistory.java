/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.util.FunctionalReadWriteLock;
import io.debezium.util.Loggings;

public abstract class AbstractFileBasedSchemaHistory extends AbstractSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileBasedSchemaHistory.class);

    protected final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    protected final AtomicBoolean running = new AtomicBoolean();
    protected final DocumentWriter documentWriter = DocumentWriter.defaultWriter();
    protected final DocumentReader documentReader = DocumentReader.defaultReader();

    private volatile List<HistoryRecord> records = new ArrayList<>();

    public AbstractFileBasedSchemaHistory() {
    }

    protected void toHistoryRecord(InputStream inputStream) {
        try (BufferedReader historyReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            while (true) {
                String line = historyReader.readLine();
                if (line == null) {
                    break;
                }
                if (!line.isEmpty()) {
                    records.add(new HistoryRecord(documentReader.read(line)));
                }
            }
        }
        catch (IOException e) {
            throw new SchemaHistoryException("Unable to read object content", e);
        }
    }

    protected byte[] fromHistoryRecord(HistoryRecord record) {
        LOGGER.trace("Storing record into database history: {}", record);

        records.add(record);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BufferedWriter historyWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
            for (HistoryRecord r : records) {
                String line = documentWriter.write(r.document());
                if (line != null) {
                    historyWriter.newLine();
                    historyWriter.append(line);
                }
            }
        }
        catch (IOException e) {
            Loggings.logErrorAndTraceRecord(logger, record, "Failed to convert record", e);
            throw new SchemaHistoryException("Failed to convert record", e);
        }
        return outputStream.toByteArray();
    }

    protected List<HistoryRecord> getRecords() {
        return records;
    }
}
