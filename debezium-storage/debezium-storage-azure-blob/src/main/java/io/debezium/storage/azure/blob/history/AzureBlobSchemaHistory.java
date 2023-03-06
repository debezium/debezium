/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.util.FunctionalReadWriteLock;
import io.debezium.util.Loggings;

/** A {@link SchemaHistory} implementation that records schema changes as normal {@link SourceRecord}s on the specified topic,
 * and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 *
 *  This implementation provides caching {@link HistoryRecord} on the main memory in the case of recovering records.
 *  {@link AzureBlobSchemaHistory#start()} fetches history log from Blob storage and store the {@link HistoryRecord} on the main memory.
 *  Also {@link AzureBlobSchemaHistory#storeRecord(HistoryRecord)} creates new history blob everytime invokes on Blob
 *
 * @author hossein torabi
 * @a
 */
public class AzureBlobSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSchemaHistory.class);

    public static final String ACCOUNT_CONNECTION_STRING_CONFIG = "azure.storage.account.connectionstring";

    public static final String CONTAINER_NAME_CONFIG = "azure.storage.account.container.name";

    public static final String BLOB_NAME_CONFIG = "azure.storage.blob.name";

    public static final Field ACCOUNT_CONNECTION_STRING = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + ACCOUNT_CONNECTION_STRING_CONFIG)
            .withDisplayName("storage connection string")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);

    public static final Field CONTAINER_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + CONTAINER_NAME_CONFIG)
            .withDisplayName("container name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);

    public static final Field BLOB_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + BLOB_NAME_CONFIG)
            .withDisplayName("blob name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);

    public static final Field.Set ALL_FIELDS = Field.setOf(ACCOUNT_CONNECTION_STRING, CONTAINER_NAME, BLOB_NAME);

    private final AtomicBoolean running = new AtomicBoolean();
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter documentWriter = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    private volatile BlobServiceClient blobServiceClient = null;
    private volatile BlobClient blobClient = null;
    private String container = null;

    private String blobName = null;

    private volatile List<HistoryRecord> records = new ArrayList<>();

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new SchemaHistoryException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        container = config.getString(CONTAINER_NAME);
        if (container == null) {
            throw new DebeziumException(CONTAINER_NAME + " is required to be set");
        }

        blobName = config.getString(BLOB_NAME);
        if (blobName == null) {
            throw new DebeziumException(BLOB_NAME + " is required to be set");
        }

    }

    @Override
    public synchronized void start() {
        if (blobServiceClient == null) {
            blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(config.getString(ACCOUNT_CONNECTION_STRING))
                    .buildClient();
        }
        if (blobClient == null) {
            blobClient = blobServiceClient.getBlobContainerClient(container)
                    .getBlobClient(blobName);
        }

        lock.write(() -> {
            if (running.compareAndSet(false, true)) {
                if (!storageExists()) {
                    initializeStorage();
                }
            }

            if (blobClient.exists()) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                blobClient.downloadStream(outputStream);
                ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
                try (BufferedReader historyReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                    while (true) {
                        String line = historyReader.readLine();
                        if (line == null) {
                            break;
                        }
                        if (!line.isEmpty()) {
                            records.add(new HistoryRecord(reader.read(line)));
                        }
                    }
                }
                catch (IOException e) {
                    throw new SchemaHistoryException("Unable to read object content", e);
                }
            }
        });
        super.start();
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (blobClient == null) {
            throw new IllegalStateException("No Blob client is available. Ensure that 'start()' is called before storing database history records.");
        }
        if (record == null) {
            return;
        }

        LOGGER.trace("Storing record into database history: {}", record);
        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }

            records.add(record);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (BufferedWriter historyWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
                for (HistoryRecord r : records) {
                    String line = null;
                    line = documentWriter.write(r.document());
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

            ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            blobClient.upload(inputStream, true);
        });
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> this.records.forEach(records));
    }

    @Override
    public boolean exists() {
        return !records.isEmpty();
    }

    @Override
    public boolean storageExists() {
        final boolean containerExists = blobServiceClient.getBlobContainerClient(container)
                .exists();

        if (containerExists) {
            LOGGER.info("Container '{}' used to store database history exists", container);
        }
        else {
            LOGGER.info("Container '{}' used to store database history does not exist yet", container);
        }
        return containerExists;
    }

    public void initializeStorage() {
        blobServiceClient.createBlobContainer(container);
    }

    @Override
    public String toString() {
        return "Azure Blob Storage";
    }
}
