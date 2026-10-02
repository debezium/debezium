/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.AbstractSchemaHistoryTest;

public class StreamingAzureBlobSchemaHistoryIT extends AbstractSchemaHistoryTest {

    private static final GenericContainer<?> AZURITE = new GenericContainer<>(
            "mcr.microsoft.com/azure-storage/azurite:" + System.getProperty("tag.azurite", "3.33.0"))
            .withCommand("azurite --blobHost 0.0.0.0")
            .withEnv("AZURITE_ACCOUNTS", "account:key")
            .withExposedPorts(10000);
    private static final String BASE = "history.log";
    private static BlobContainerClient container;

    @BeforeAll
    public static void startAzurite() {
        AZURITE.start();
        container = new BlobServiceClientBuilder().connectionString(connectionString()).buildClient()
                .getBlobContainerClient("streaming-history");
    }

    @AfterAll
    public static void stopAzurite() {
        AZURITE.stop();
    }

    @AfterEach
    public void cleanup() {
        if (history != null) {
            history.stop();
        }
        container.deleteIfExists();
    }

    @Override
    protected SchemaHistory createHistory() {
        final var result = configured();
        result.start();
        return result;
    }

    @Test
    public void shouldPreserveLegacyBytesAndRecoverBaseThenTailAfterRestart() throws Exception {
        history.stop();
        final String legacy = "\n" + json(1, "CREATE TABLE foo (id INT);");
        base().upload(BinaryData.fromString(legacy), true);
        container.getBlobClient(BASE + ".streaming.json").delete();
        history = createHistory();
        final String etag = base().getProperties().getETag();
        record(2, 0, "ALTER TABLE foo ADD name VARCHAR(20);");
        assertThat(base().getProperties().getETag()).isEqualTo(etag);
        assertThat(base().downloadContent().toString()).isEqualTo(legacy);
        history.stop();
        history = createHistory();
        assertThat(recover(1, 0).forTable(io.debezium.relational.TableId.parse("db.foo")).columns()).hasSize(1);
        assertThat(recover(2, 0).forTable(io.debezium.relational.TableId.parse("db.foo")).columns()).hasSize(2);
        assertThat(tail().downloadContent().toString()).startsWith("\n{");
    }

    @Test
    public void shouldHandleCrLfAndFinalLineWithoutDelimiter() throws Exception {
        reinitializeBase("\r\n" + json(1, "CREATE TABLE foo (id INT);") + "\r\n \r\n"
                + json(2, "ALTER TABLE foo ADD name VARCHAR(20);"));
        assertThat(history.exists()).isTrue();
        assertThat(recover(2, 0).tableIds()).hasSize(1);
    }

    @Test
    public void shouldReportBlankHistoryAsAbsent() {
        reinitializeBase("\r\n  \n\t\n");
        assertThat(history.exists()).isFalse();
    }

    @Test
    public void shouldRejectCompetingWriterAndKeepOwnerUsable() {
        final var competitor = configured();
        assertThatThrownBy(competitor::start).isInstanceOf(SchemaHistoryException.class);
        competitor.stop();
        record(1, 0, "CREATE TABLE foo (id INT);");
        assertThat(history.exists()).isTrue();
    }

    @Test
    public void shouldFenceLegacyOverwriteWhileRunning() {
        assertThatThrownBy(() -> base().upload(BinaryData.fromString("{}"), true)).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void shouldRejectChangedBaseAfterStop() {
        history.stop();
        base().upload(BinaryData.fromString("\n{}"), true);
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Legacy history changed after streaming layout initialization");
        next.stop();
    }

    @Test
    public void shouldRejectMissingReferencedTail() {
        record(1, 0, "CREATE TABLE foo (id INT);");
        history.stop();
        tail().delete();
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Streaming history references a missing append tail");
        next.stop();
    }

    @Test
    public void shouldRejectMissingReferencedBase() {
        history.stop();
        base().delete();
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Streaming history references a missing legacy base");
        next.stop();
    }

    @Test
    public void shouldRejectOversizedDescriptorBeforeDownloadingIt() {
        history.stop();
        container.getBlobClient(BASE + ".streaming.json").upload(BinaryData.fromString(" ".repeat(4097)), true);
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Streaming Azure history descriptor exceeds 4 KiB");
        next.stop();
    }

    @Test
    public void shouldFailClosedAfterWriterLeaseIsBroken() {
        new BlobLeaseClientBuilder().blobClient(tail()).buildClient()
                .breakLeaseWithResponse(0, null, java.time.Duration.ofSeconds(10), com.azure.core.util.Context.NONE);
        assertThatThrownBy(() -> history.record(source1, position("a.log", 1, 0), "db", "CREATE TABLE foo (id INT);"))
                .isInstanceOf(SchemaHistoryException.class);
        assertThatThrownBy(history::exists).isInstanceOf(SchemaHistoryException.class)
                .hasMessageContaining("lease lost");
        assertThat(tail().getProperties().getBlobSize()).isZero();
    }

    @Test
    public void shouldRejectInvalidUtf8() {
        history.stop();
        container.getBlobClient(BASE + ".streaming.json").delete();
        base().upload(BinaryData.fromBytes(new byte[]{ (byte) 0xc3, (byte) 0x28 }), true);
        history = createHistory();
        assertThatThrownBy(() -> recover(10, 0)).isInstanceOf(SchemaHistoryException.class)
                .hasCauseInstanceOf(java.nio.charset.CharacterCodingException.class);
    }

    @Test
    public void shouldRejectUnknownDescriptorVersion() throws IOException {
        history.stop();
        final BlobClient descriptor = container.getBlobClient(BASE + ".streaming.json");
        final Document json = io.debezium.document.DocumentReader.defaultReader().read(descriptor.downloadContent().toString());
        json.setNumber("formatVersion", 999);
        descriptor.upload(BinaryData.fromString(DocumentWriter.defaultWriter().write(json)), true);
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class);
        next.stop();
    }

    @Test
    public void shouldFailOnCorruptHistoryInsteadOfSkippingIt() {
        reinitializeBase("\n{\"source\":");
        assertThatThrownBy(() -> recover(10, 0)).isInstanceOf(SchemaHistoryException.class);
    }

    @Test
    public void shouldHonorRecoveryInterruption() {
        record(1, 0, "CREATE TABLE foo (id INT);");
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> recover(10, 0)).isInstanceOf(InterruptedException.class);
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    public void shouldRejectOversizedLineBeforeParsing() {
        reinitializeBase("x".repeat(4 * 1024 * 1024 + 1));
        assertThatThrownBy(() -> recover(10, 0)).isInstanceOf(SchemaHistoryException.class)
                .hasMessageContaining("larger than 4 MiB");
    }

    @Test
    public void shouldRecoverManyRecordsWithoutRetainedList() throws Exception {
        final var content = new StringBuilder();
        for (int i = 0; i < 2000; ++i) {
            content.append('\n').append(json(i, "CREATE TABLE foo (id INT);"));
        }
        reinitializeBase(content.toString());
        assertThat(recover(3000, 0).tableIds()).hasSize(1);
        assertThat(java.util.Arrays.stream(StreamingAzureBlobSchemaHistory.class.getDeclaredFields())
                .noneMatch(field -> field.getName().equals("records"))).isTrue();
    }

    private void reinitializeBase(String content) {
        history.stop();
        container.getBlobClient(BASE + ".streaming.json").delete();
        base().upload(BinaryData.fromString(content), true);
        history = createHistory();
    }

    private String json(long pos, String ddl) throws IOException {
        return DocumentWriter.defaultWriter().write(new HistoryRecord(source1, position("a.log", pos, 0),
                "db", null, ddl, null, null).document());
    }

    private static BlobClient base() {
        return container.getBlobClient(BASE);
    }

    private static BlobClient tail() {
        return container.getBlobClient(BASE + ".streaming.tail");
    }

    private static StreamingAzureBlobSchemaHistory configured() {
        final var result = new StreamingAzureBlobSchemaHistory();
        result.configure(Configuration.create()
                .with(AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING, connectionString())
                .with(AzureBlobSchemaHistory.CONTAINER_NAME, "streaming-history")
                .with(AzureBlobSchemaHistory.BLOB_NAME, BASE).build(), null, SchemaHistoryListener.NOOP, true);
        return result;
    }

    private static String connectionString() {
        return String.format("DefaultEndpointsProtocol=http;AccountName=account;AccountKey=key;BlobEndpoint=http://127.0.0.1:%s/account;",
                AZURITE.getMappedPort(10000));
    }
}
