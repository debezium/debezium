/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static io.debezium.util.Strings.isNullOrEmpty;
import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.AbstractFileBasedSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;

/** A {@link SchemaHistory} implementation that records schema changes as normal {@link SourceRecord}s on the specified topic,
 * and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 *
 *  This implementation provides caching {@link HistoryRecord} on the main memory in the case of recovering records.
 *  {@link AzureBlobSchemaHistory#start()} fetches history log from Blob storage and store the {@link HistoryRecord} on the main memory.
 *  Also {@link AzureBlobSchemaHistory#storeRecord(HistoryRecord)} creates new history blob everytime invokes on Blob
 *
 * @author hossein torabi
 */
public class AzureBlobSchemaHistory extends AbstractFileBasedSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSchemaHistory.class);

    public static final String ACCOUNT_CONNECTION_STRING_CONFIG = "azure.storage.account.connectionstring";

    public static final String ACCOUNT_NAME_CONFIG = "azure.storage.account.name";

    public static final String CONTAINER_NAME_CONFIG = "azure.storage.account.container.name";

    public static final String BLOB_NAME_CONFIG = "azure.storage.blob.name";

    public static final Field ACCOUNT_CONNECTION_STRING = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + ACCOUNT_CONNECTION_STRING_CONFIG)
            .withDisplayName("storage connection string")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);

    public static final Field ACCOUNT_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + ACCOUNT_NAME_CONFIG)
            .withDisplayName("account name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);

    public static final Field CONTAINER_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + CONTAINER_NAME_CONFIG)
            .withDisplayName("container name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required();

    public static final Field BLOB_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + BLOB_NAME_CONFIG)
            .withDisplayName("blob name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .required();

    public static final Field.Set ALL_FIELDS = Field.setOf(ACCOUNT_CONNECTION_STRING, ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME);

    private static final String ENDPOINT_FORMAT = "https://%s.blob.core.windows.net";

    private volatile BlobServiceClient blobServiceClient = null;
    private volatile BlobClient blobClient = null;
    private String container;
    private String blobName;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        container = config.getString(CONTAINER_NAME);
        blobName = config.getString(BLOB_NAME);
    }

    @Override
    protected void doPreStart() {
        if (blobServiceClient == null) {
            if (isNullOrEmpty(config.getString(ACCOUNT_CONNECTION_STRING))) {
                blobServiceClient = new BlobServiceClientBuilder()
                        .endpoint(format(ENDPOINT_FORMAT, config.getString(ACCOUNT_NAME)))
                        .credential(new DefaultAzureCredentialBuilder().build())
                        .buildClient();
            }
            else {
                blobServiceClient = new BlobServiceClientBuilder()
                        .connectionString(config.getString(ACCOUNT_CONNECTION_STRING))
                        .buildClient();
            }
        }

        if (blobClient == null) {
            blobClient = blobServiceClient.getBlobContainerClient(container)
                    .getBlobClient(blobName);
        }
    }

    @Override
    protected void doStart() {
        if (blobClient.exists()) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            blobClient.downloadStream(outputStream);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            toHistoryRecord(inputStream);
        }
    }

    @Override
    protected void doPreStoreRecord(HistoryRecord record) {
        if (blobClient == null) {
            throw new SchemaHistoryException("No Blob client is available. Ensure that 'start()' is called before storing database history records.");
        }
    }

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        blobClient.upload(new ByteArrayInputStream(fromHistoryRecord(record)), true);
    }

    @Override
    public boolean storageExists() {
        final boolean containerExists = blobServiceClient.getBlobContainerClient(container).exists();

        if (containerExists) {
            LOGGER.info("Container '{}' used to store database history exists", container);
        }
        else {
            LOGGER.info("Container '{}' used to store database history does not exist yet", container);
        }
        return containerExists;
    }

    public void initializeStorage() {
        LOGGER.info("Creating Azure Blob container '{}' used to store database history", container);
        blobServiceClient.createBlobContainer(container);
    }

    @Override
    public String toString() {
        return "Azure Blob Storage";
    }
}
