/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.AbstractSchemaHistoryTest;

public class AzureBlobSchemaHistoryIT extends AbstractSchemaHistoryTest {

    final public static String IMAGE_TAG = System.getProperty("tag.azurite", "latest");

    final public static String CONTAINER_NAME = "debezium";

    final public static String BLOB_NAME = "debezium-history.log";

    final public static String CONNECTION_STRING = "DefaultEndpointsProtocol=http;" +
            "AccountName=account;" +
            "AccountKey=key;" +
            "BlobEndpoint=http://127.0.0.1:%s/account;";

    final private static GenericContainer<?> container = new GenericContainer(String.format("mcr.microsoft.com/azure-storage/azurite:%s", IMAGE_TAG))
            .withCommand("azurite --blobHost 0.0.0.0 --blobPort 10000")
            .withEnv("AZURITE_ACCOUNTS", "account:key")
            .withExposedPorts(10000);

    private static BlobServiceClient blobServiceClient;

    @BeforeClass
    public static void startAzurite() {
        container.start();
        blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(String.format(CONNECTION_STRING, container.getMappedPort(10000)))
                .buildClient();
    }

    @AfterClass()
    public static void stopAzurite() {
        container.stop();
    }

    @Override
    protected SchemaHistory createHistory() {
        SchemaHistory history = new AzureBlobSchemaHistory();
        Configuration config = Configuration.create()
                .with(AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING, String.format(CONNECTION_STRING, container.getMappedPort(10000)))
                .with(AzureBlobSchemaHistory.CONTAINER_NAME, CONTAINER_NAME)
                .with(AzureBlobSchemaHistory.BLOB_NAME, BLOB_NAME)
                .build();

        history.configure(config, null, SchemaHistoryListener.NOOP, true);
        history.start();

        return history;

    }

    @Test
    public void initializeStorageShouldCreateContainer() {
        blobServiceClient.deleteBlobContainer(CONTAINER_NAME);

        assertFalse(blobServiceClient.getBlobContainerClient(CONTAINER_NAME).exists());
        history.initializeStorage();
        assertTrue(blobServiceClient.getBlobContainerClient(CONTAINER_NAME).exists());
    }

    @Test
    public void storeRecordShouldSaveRecordsInBlobStorage() throws IOException {
        BlobClient blobClient = blobServiceClient
                .getBlobContainerClient(CONTAINER_NAME)
                .getBlobClient(BLOB_NAME);

        assertFalse(blobClient.exists());
        record(01, 0, "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", all, t3, t2, t1, t0);
        assertTrue(blobClient.exists());

        List<HistoryRecord> historyRecords = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(blobClient.downloadContent().toBytes());
        BufferedReader historyReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        DocumentReader reader = DocumentReader.defaultReader();
        while (true) {
            String line = historyReader.readLine();
            if (line == null) {
                break;
            }
            historyRecords.add(new HistoryRecord(reader.read(historyReader.readLine())));
        }

        assertEquals(1, historyRecords.size());

        assertEquals("CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", historyRecords.get(0).document().getString("ddl"));
        assertEquals(1, historyRecords.get(0).document().getDocument("position").getInteger("position").intValue());
        assertEquals(0, historyRecords.get(0).document().getDocument("position").getInteger("entry").intValue());
    }
}
