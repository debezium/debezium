/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.AbstractSchemaHistoryTest;

public class VersionedAzureBlobSchemaHistoryIT extends AbstractSchemaHistoryTest {

    private static final String IMAGE_TAG = System.getProperty("tag.azurite", "latest");
    private static final String CONTAINER_NAME = "debezium-versioned";
    private static final String BLOB_NAME = "debezium-history.log";
    private static final String LAYOUT_PREFIX = BLOB_NAME + ".versioned-poc";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=http;" +
            "AccountName=account;" +
            "AccountKey=key;" +
            "BlobEndpoint=http://127.0.0.1:%s/account;";

    private static final GenericContainer<?> CONTAINER = new GenericContainer(String.format("mcr.microsoft.com/azure-storage/azurite:%s", IMAGE_TAG))
            .withCommand("azurite --blobHost 0.0.0.0 --blobPort 10000")
            .withEnv("AZURITE_ACCOUNTS", "account:key")
            .withExposedPorts(10000);

    private static BlobServiceClient blobServiceClient;

    @BeforeAll
    public static void startAzurite() {
        CONTAINER.start();
        blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString())
                .buildClient();
    }

    @AfterAll
    public static void stopAzurite() {
        CONTAINER.stop();
    }

    @AfterEach
    public void cleanupBlobStorage() {
        try {
            if (containerClient().exists()) {
                blobServiceClient.deleteBlobContainer(CONTAINER_NAME);
            }
        }
        catch (Exception e) {
            // Ignore cleanup errors.
        }
    }

    @Override
    protected SchemaHistory createHistory() {
        final SchemaHistory history = new VersionedAzureBlobSchemaHistory();
        history.configure(configuration(), null, SchemaHistoryListener.NOOP, true);
        history.start();
        return history;
    }

    @Test
    public void shouldPromoteOneFinalRecordPerTable() throws Exception {
        final Table first = table("customers", "name");
        final Table second = table("customers", "name", "email");

        history.startBuffering();
        recordTable(1, new TableChanges().create(first));
        recordTable(2, new TableChanges().alter(second));
        history.stopBuffering(true);

        assertThat(checkpointLines()).hasSize(1);
        assertThat(recover(2, 0).forTable(second.id())).isEqualTo(second);
    }

    @Test
    public void shouldIgnoreAbortedPendingCheckpoint() throws Exception {
        final Table active = table("customers", "name");
        final Table pending = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(active));
        history.stopBuffering(true);

        history.startBuffering();
        recordTable(2, new TableChanges().create(pending));
        history.stopBuffering(false);

        final Tables recovered = recover(10, 0);
        assertThat(recovered.forTable(active.id())).isEqualTo(active);
        assertThat(recovered.forTable(pending.id())).isNull();
        assertThat(versionedHistory().cleanupOrphanBlobs()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void shouldIgnorePendingCheckpointAfterCrash() throws Exception {
        final Table active = table("customers", "name");
        final Table pending = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(active));
        history.stopBuffering(true);

        history.startBuffering();
        recordTable(2, new TableChanges().create(pending));
        history.stop();
        history = createHistory();

        final Tables recovered = recover(10, 0);
        assertThat(recovered.forTable(active.id())).isEqualTo(active);
        assertThat(recovered.forTable(pending.id())).isNull();
    }

    @Test
    public void shouldReplacePendingCheckpointWhenNoSnapshotSucceeded() throws Exception {
        final Table abandoned = table("customers", "name");
        final Table retried = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(abandoned));
        history.stop();
        history = createHistory();

        assertThat(recover(10, 0).tableIds()).isEmpty();

        history.startBuffering();
        recordTable(2, new TableChanges().create(retried));
        history.stopBuffering(true);

        final Tables recovered = recover(10, 0);
        assertThat(recovered.forTable(abandoned.id())).isNull();
        assertThat(recovered.forTable(retried.id())).isEqualTo(retried);
        assertThat(versionedHistory().cleanupOrphanBlobs()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void shouldRemoveDroppedTableFromPendingCheckpoint() throws Exception {
        final Table dropped = table("customers", "name");
        final Table retained = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(dropped));
        recordTable(2, new TableChanges().create(retained));
        recordTable(3, new TableChanges().drop(dropped.id()));
        history.stopBuffering(true);

        final Tables recovered = recover(10, 0);
        assertThat(recovered.forTable(dropped.id())).isNull();
        assertThat(recovered.forTable(retained.id())).isEqualTo(retained);
        assertThat(checkpointLines()).hasSize(1);
    }

    @Test
    public void shouldReplayStreamingDeltasAfterCheckpoint() throws Exception {
        final Table checkpoint = table("customers", "name");
        final Table altered = table("customers", "name", "email");
        final Table created = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(checkpoint));
        history.stopBuffering(true);

        recordTable(2, new TableChanges().alter(altered));
        recordTable(3, new TableChanges().create(created));

        final Tables recovered = recover(3, 0);
        assertThat(recovered.forTable(altered.id())).isEqualTo(altered);
        assertThat(recovered.forTable(created.id())).isEqualTo(created);
        assertThat(deltaBlobNames()).hasSize(2);
    }

    @Test
    public void shouldMergePartialBufferedSchemaWithActiveCheckpoint() throws Exception {
        final Table active = table("customers", "name");
        final Table partial = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(active));
        history.stopBuffering(true);

        history.startBuffering();
        recordTable(2, new TableChanges().create(partial));
        history.stopBuffering(true);

        final Tables recovered = recover(10, 0);
        assertThat(recovered.forTable(active.id())).isEqualTo(active);
        assertThat(recovered.forTable(partial.id())).isEqualTo(partial);
        assertThat(checkpointLines()).hasSize(2);
    }

    @Test
    public void shouldCompactMultiTableRecordAtTableGranularity() throws Exception {
        final Table dropped = table("customers", "name");
        final Table retained = table("orders", "description");

        history.startBuffering();
        recordTable(1, new TableChanges().create(dropped).create(retained));
        recordTable(2, new TableChanges().drop(dropped.id()));
        history.stopBuffering(true);

        final Tables recovered = recover(10, 0);
        assertThat(recovered.forTable(dropped.id())).isNull();
        assertThat(recovered.forTable(retained.id())).isEqualTo(retained);
        assertThat(checkpointLines()).hasSize(1);
    }

    @Test
    public void shouldMigrateLegacyHistoryIntoCompactCheckpoint() throws Exception {
        history.stop();

        final Table first = table("customers", "name");
        final Table second = table("customers", "name", "email");
        final String legacyContent = serialize(historyRecord(1, new TableChanges().create(first))) +
                serialize(historyRecord(2, new TableChanges().alter(second)));
        containerClient().getBlobClient(BLOB_NAME).upload(BinaryData.fromString(legacyContent), true);

        history = createHistory();
        assertTrue(versionedHistory().migrateLegacyHistory());
        assertFalse(versionedHistory().migrateLegacyHistory());

        assertThat(checkpointLines()).hasSize(1);
        assertThat(recover(2, 0).forTable(second.id())).isEqualTo(second);
        assertTrue(containerClient().getBlobClient(BLOB_NAME).exists());
    }

    @Test
    public void shouldIgnoreOrphanDeltaBlob() throws Exception {
        final Table checkpoint = table("customers", "name");
        history.startBuffering();
        recordTable(1, new TableChanges().create(checkpoint));
        history.stopBuffering(true);

        final String orphanName = LAYOUT_PREFIX + "/deltas/orphan.jsonl";
        containerClient().getBlobClient(orphanName).upload(BinaryData.fromString(
                serialize(historyRecord(2, new TableChanges().create(table("orders", "description"))))), true);

        assertThat(recover(10, 0).forTable(TableId.parse("db.public.orders"))).isNull();
        assertThat(versionedHistory().cleanupOrphanBlobs()).isEqualTo(1);
        assertFalse(containerClient().getBlobClient(orphanName).exists());
    }

    private VersionedAzureBlobSchemaHistory versionedHistory() {
        return (VersionedAzureBlobSchemaHistory) history;
    }

    private void recordTable(long position, TableChanges changes) {
        history.record(
                source1,
                position("a.log", position, 0),
                "db",
                "public",
                null,
                changes,
                Instant.ofEpochMilli(position));
    }

    private HistoryRecord historyRecord(long position, TableChanges changes) {
        return new HistoryRecord(
                source1,
                position("a.log", position, 0),
                "db",
                "public",
                null,
                changes,
                Instant.ofEpochMilli(position));
    }

    private String serialize(HistoryRecord record) throws IOException {
        return DocumentWriter.defaultWriter().write(record.document()) + System.lineSeparator();
    }

    private Table table(String name, String... columns) {
        final TableEditor editor = Table.editor()
                .tableId(TableId.parse("db.public." + name));
        for (int i = 0; i < columns.length; ++i) {
            editor.addColumn(Column.editor()
                    .name(columns[i])
                    .jdbcType(java.sql.Types.VARCHAR)
                    .type("VARCHAR")
                    .position(i + 1)
                    .optional(true)
                    .create());
        }
        return editor.create();
    }

    private List<String> checkpointLines() {
        final String checkpoint = checkpointBlobNames().get(0);
        return containerClient().getBlobClient(checkpoint)
                .downloadContent()
                .toString()
                .lines()
                .filter(line -> !line.isBlank())
                .toList();
    }

    private List<String> checkpointBlobNames() {
        return blobNames(LAYOUT_PREFIX + "/checkpoints/");
    }

    private List<String> deltaBlobNames() {
        return blobNames(LAYOUT_PREFIX + "/deltas/");
    }

    private List<String> blobNames(String prefix) {
        return containerClient()
                .listBlobs(new ListBlobsOptions().setPrefix(prefix), null)
                .stream()
                .map(item -> item.getName())
                .sorted()
                .toList();
    }

    private static Configuration configuration() {
        return Configuration.create()
                .with(VersionedAzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING, connectionString())
                .with(VersionedAzureBlobSchemaHistory.CONTAINER_NAME, CONTAINER_NAME)
                .with(VersionedAzureBlobSchemaHistory.BLOB_NAME, BLOB_NAME)
                .build();
    }

    private static BlobContainerClient containerClient() {
        return blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
    }

    private static String connectionString() {
        return String.format(CONNECTION_STRING, CONTAINER.getMappedPort(10000));
    }
}
