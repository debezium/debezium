/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;

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
        base().setMetadata(Map.of());
        tail().delete();
        history = createHistory();
        final String etag = base().getProperties().getETag();
        record(2, 0, "ALTER TABLE foo ADD name VARCHAR(20);");
        assertThat(base().getProperties().getETag()).isEqualTo(etag);
        assertThat(base().downloadContent().toString()).isEqualTo(legacy);
        history.stop();
        history = createHistory();
        assertThat(recover(1, 0).forTable(io.debezium.relational.TableId.parse("db.foo")).columns()).hasSize(1);
        assertThat(recover(2, 0).forTable(io.debezium.relational.TableId.parse("db.foo")).columns()).hasSize(2);
        assertThat(tail().downloadContent().toString()).startsWith("{").endsWith("\n");
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
        final Map<String, String> marker = base().getProperties().getMetadata();
        base().upload(BinaryData.fromString("\n{}"), true);
        base().setMetadata(marker);
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Legacy history changed after streaming layout initialization");
        next.stop();
    }

    @Test
    public void shouldInitializeExistingBaseWithoutTail() throws Exception {
        history.stop();
        tail().delete();
        final String legacy = "\n" + json(1, "CREATE TABLE foo (id INT);");
        base().upload(BinaryData.fromString(legacy), true);
        base().setMetadata(Map.of());
        history = createHistory();
        assertThat(base().downloadContent().toString()).isEqualTo(legacy);
        assertThat(tail().getProperties().getBlobSize()).isZero();
        assertThat(recover(1, 0).tableIds()).hasSize(1);
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
    public void shouldInitializeEmptyOrphanTailWithMissingOrIncompleteMetadata() {
        history.stop();
        for (Map<String, String> incomplete : java.util.List.of(Map.<String, String> of(),
                Map.of("formatVersion", "jsonl-1"), Map.of("baseETag", base().getProperties().getETag()))) {
            tail().setMetadata(incomplete);
            history = createHistory();
            assertThat(tail().getProperties().getMetadata()).hasSize(4);
            assertThat(history.exists()).isFalse();
            history.stop();
        }
    }

    @Test
    public void shouldFailClosedAfterWriterLeaseIsBroken() {
        new BlobLeaseClientBuilder().blobClient(tail()).buildClient()
                .breakLeaseWithResponse(0, null, java.time.Duration.ofSeconds(10), com.azure.core.util.Context.NONE);
        assertThatThrownBy(() -> history.record(source1, position("a.log", 1, 0), "db", "CREATE TABLE foo (id INT);"))
                .isInstanceOf(SchemaHistoryException.class);
        assertThatThrownBy(history::exists).isInstanceOf(SchemaHistoryException.class)
                .hasMessageContaining("History append failed");
        assertThat(tail().getProperties().getBlobSize()).isZero();
    }

    @Test
    public void shouldRejectInvalidUtf8() {
        history.stop();
        tail().delete();
        base().upload(BinaryData.fromBytes(new byte[]{ (byte) 0xc3, (byte) 0x28 }), true);
        base().setMetadata(Map.of());
        history = createHistory();
        assertThatThrownBy(() -> recover(10, 0)).isInstanceOf(SchemaHistoryException.class)
                .hasMessageContaining("Unable to stream");
    }

    @Test
    public void shouldRejectUnsupportedMetadataVersion() {
        history.stop();
        tail().setMetadata(Map.of("formatVersion", "999", "baseETag", base().getProperties().getETag(), "baseLength", "0"));
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Unsupported JSONL history tail metadata; repair required");
        next.stop();
    }

    @Test
    public void shouldInitializeOnlyBaseAndJsonlTailForNewHistory() throws Exception {
        assertThat(base().getProperties().getBlobSize()).isZero();
        assertThat(tail().getProperties().getBlobSize()).isZero();
        assertThat(history.exists()).isFalse();
        assertThat(tail().getProperties().getMetadata().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(entry -> entry.getKey().toLowerCase(java.util.Locale.ROOT), Map.Entry::getValue)))
                .containsExactlyInAnyOrderEntriesOf(Map.of("formatversion", "jsonl-1", "baseetag", base().getProperties().getETag(), "baselength", "0",
                        "layoutid", metadata(base()).get("dbzhistorylayoutid")));
        assertThat(metadata(base())).containsEntry("dbzhistorytail", BASE + ".streaming.tail").containsEntry("dbzhistoryformat", "jsonl-1");
        assertThat(container.listBlobs().stream().map(item -> item.getName()))
                .containsExactlyInAnyOrder(BASE, BASE + ".streaming.tail");
        record(1, 0, "CREATE TABLE foo (id INT);");
        history.stop();
        history = createHistory();
        assertThat(recover(1, 0).tableIds()).hasSize(1);
    }

    @Test
    public void shouldRejectTruncatedTailBeforeAdmittingWrites() {
        history.stop();
        tail().getAppendBlobClient().appendBlock(BinaryData.fromString("\n{}").toStream(), 3);
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasMessageContaining("Unable to start");
        next.stop();
        assertThat(tail().getProperties().getBlobSize()).isEqualTo(3);
    }

    @Test
    public void shouldRejectMalformedOrInconsistentMetadataWithoutChangingStorage() {
        history.stop();
        for (Map<String, String> unsupported : java.util.List.of(
                Map.of("formatVersion", "jsonl-1", "baseETag", base().getProperties().getETag(), "baseLength", "not-a-number"),
                Map.of("formatVersion", "jsonl-1", "baseETag", "changed", "baseLength", "0"),
                Map.of("formatVersion", "jsonl-1", "baseETag", base().getProperties().getETag(), "baseLength", "1"),
                Map.of("unexpected", "field"))) {
            tail().setMetadata(unsupported);
            final String etag = tail().getProperties().getETag();
            final var next = configured();
            assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class);
            next.stop();
            assertThat(tail().getProperties().getETag()).isEqualTo(etag);
            assertThat(tail().getProperties().getBlobSize()).isZero();
        }
    }

    @Test
    public void shouldRejectNonemptyTailWithMissingOrIncompleteMetadata() {
        record(1, 0, "CREATE TABLE foo (id INT);");
        history.stop();
        for (Map<String, String> incomplete : java.util.List.of(Map.<String, String> of(), Map.of("formatVersion", "jsonl-1"))) {
            tail().setMetadata(incomplete);
            final var next = configured();
            assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                    .hasRootCauseMessage("Nonempty JSONL tail has incomplete metadata; repair required");
            next.stop();
        }
    }

    @Test
    public void shouldRejectDeletedTailAndConfiguredTailNameChange() {
        history.stop();
        final var renamed = configured(BASE + ".other-tail");
        assertThatThrownBy(renamed::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Unsupported base layout marker or configured tail name changed; repair required");
        renamed.stop();
        assertThat(container.getBlobClient(BASE + ".other-tail").exists()).isFalse();
        tail().delete();
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Initialized history tail is missing; history loss requires repair");
        next.stop();
        assertThat(tail().exists()).isFalse();
    }

    @Test
    public void shouldFinishHandshakeAfterBaseMarkerButBeforeTailMetadata() {
        history.stop();
        final String etag = base().getProperties().getETag();
        final String layout = metadata(base()).get("dbzhistorylayoutid");
        tail().setMetadata(Map.of());
        history = createHistory();
        assertThat(base().getProperties().getETag()).isEqualTo(etag);
        assertThat(metadata(tail())).containsEntry("layoutid", layout).containsEntry("baseetag", etag);
    }

    @Test
    public void shouldRejectMismatchedLayoutIdsEvenOnEmptyTail() {
        history.stop();
        final Map<String, String> metadata = new java.util.HashMap<>(metadata(tail()));
        metadata.put("layoutid", java.util.UUID.randomUUID().toString());
        tail().setMetadata(metadata);
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Base and tail layout IDs mismatch; repair required");
        next.stop();
    }

    @Test
    public void shouldRejectLegacyOverwriteThatRemovesBaseMarker() {
        record(1, 0, "CREATE TABLE foo (id INT);");
        history.stop();
        base().upload(BinaryData.fromString("{}"), true);
        base().setMetadata(Map.of());
        final var next = configured();
        assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Unmarked base has an initialized or nonempty tail; repair required");
        next.stop();
    }

    @Test
    public void shouldPreserveExistingBaseMetadataAndPinPostMarkerEtag() {
        history.stop();
        tail().delete();
        base().setMetadata(Map.of("owner", "test-owner"));
        final String before = base().getProperties().getETag();
        history = createHistory();
        final String after = base().getProperties().getETag();
        assertThat(after).isNotEqualTo(before);
        assertThat(metadata(base())).containsEntry("owner", "test-owner");
        assertThat(metadata(tail())).containsEntry("baseetag", after)
                .containsEntry("layoutid", metadata(base()).get("dbzhistorylayoutid"));
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
    public void shouldRejectOversizedLegacyLine() {
        reinitializeBase("x".repeat(4 * 1024 * 1024 + 1));
        assertThatThrownBy(() -> recover(10, 0)).isInstanceOf(SchemaHistoryException.class)
                .hasMessageContaining("4 MiB append block");
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

    @Test
    public void shouldAppendAtExactBlockBoundaryAndRejectOversizeBeforeWrite() throws Exception {
        final var record = new HistoryRecord(source1, position("a.log", 1, 0), "db", null, null, null, null);
        record.document().setString("unknown", "");
        final int overhead = DocumentWriter.defaultWriter().write(record.document()).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        record.document().setString("unknown", "x".repeat(StreamingAzureBlobSchemaHistory.MAX_RECORD_BYTES - overhead));
        ((StreamingAzureBlobSchemaHistory) history).storeRecord(record);
        assertThat(tail().getProperties().getBlobSize()).isEqualTo(4 * 1024 * 1024);
        assertThat(tail().getProperties().getCommittedBlockCount()).isEqualTo(1);
        final var recovered = new java.util.ArrayList<HistoryRecord>();
        ((StreamingAzureBlobSchemaHistory) history).recoverRecords(recovered::add);
        assertThat(recovered).hasSize(1);
        assertThat(recovered.get(0).document().getString("unknown")).isEqualTo(record.document().getString("unknown"));
        record.document().setString("unknown", record.document().getString("unknown") + "x");
        assertThatThrownBy(() -> ((StreamingAzureBlobSchemaHistory) history).storeRecord(record))
                .isInstanceOf(SchemaHistoryException.class).hasRootCauseMessage(
                        "Azure schema-history record exceeds 4194303 UTF-8 bytes (4 MiB append block minus newline); reduce the record or export/remediate history before restarting");
        assertThat(tail().getProperties().getBlobSize()).isEqualTo(4 * 1024 * 1024);
    }

    @Test
    public void shouldReconcileLostAppendResponseWithoutDuplicatingRecord() throws Exception {
        history.stop();
        final var lost = new StreamingAzureBlobSchemaHistory() {
            @Override
            protected void appendBlock(byte[] payload, com.azure.storage.blob.models.AppendBlobRequestConditions conditions) {
                super.appendBlock(payload, conditions);
                throw new IllegalStateException("injected lost response");
            }
        };
        lost.configure(Configuration.create().with(AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING, connectionString())
                .with(AzureBlobSchemaHistory.CONTAINER_NAME, "streaming-history").with(AzureBlobSchemaHistory.BLOB_NAME, BASE).build(),
                null, SchemaHistoryListener.NOOP, true);
        history = lost;
        history.start();
        record(1, 0, "CREATE TABLE foo (id INT);");
        assertThat(tail().getProperties().getCommittedBlockCount()).isEqualTo(1);
        history.stop();
        history = createHistory();
        assertThat(recover(1, 0).tableIds()).hasSize(1);
    }

    @Test
    public void shouldRejectMalformedTailAndTrailingJson() throws Exception {
        history.stop();
        for (String content : new String[]{ "{\"source\":\n", "{} {}\n" }) {
            final var blob = tail().getAppendBlobClient();
            blob.delete();
            blob.create();
            blob.setMetadata(Map.of("formatVersion", "jsonl-1", "layoutId", metadata(base()).get("dbzhistorylayoutid"),
                    "baseETag", base().getProperties().getETag(), "baseLength", "0"));
            final byte[] bytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            blob.appendBlock(new java.io.ByteArrayInputStream(bytes), bytes.length);
            final var next = configured();
            assertThatThrownBy(next::start).isInstanceOf(SchemaHistoryException.class);
            next.stop();
        }
    }

    @Test
    public void shouldFailAtAppendBlockLimitWithoutWriting() throws Exception {
        final var field = StreamingAzureBlobSchemaHistory.class.getDeclaredField("tailBlocks");
        field.setAccessible(true);
        field.setInt(history, tail().getAppendBlobClient().getMaxBlocks());
        assertThatThrownBy(() -> history.record(source1, position("a.log", 1, 0), "db", "CREATE TABLE foo (id INT);"))
                .isInstanceOf(SchemaHistoryException.class)
                .hasRootCauseMessage("Schema-history tail reached Azure's append-block limit; export to a new history before continuing");
        assertThat(tail().getProperties().getBlobSize()).isZero();
    }

    private void reinitializeBase(String content) {
        history.stop();
        tail().delete();
        base().upload(BinaryData.fromString(content), true);
        base().setMetadata(Map.of());
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
        return configured(BASE + ".streaming.tail");
    }

    private static StreamingAzureBlobSchemaHistory configured(String tailName) {
        final var result = new StreamingAzureBlobSchemaHistory();
        result.configure(Configuration.create()
                .with(AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING, connectionString())
                .with(AzureBlobSchemaHistory.CONTAINER_NAME, "streaming-history")
                .with(StreamingAzureBlobSchemaHistory.TAIL_NAME, tailName)
                .with(AzureBlobSchemaHistory.BLOB_NAME, BASE).build(), null, SchemaHistoryListener.NOOP, true);
        return result;
    }

    private static Map<String, String> metadata(BlobClient blob) {
        return blob.getProperties().getMetadata().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(entry -> entry.getKey().toLowerCase(java.util.Locale.ROOT), Map.Entry::getValue));
    }

    private static String connectionString() {
        return String.format("DefaultEndpointsProtocol=http;AccountName=account;AccountKey=key;BlobEndpoint=http://127.0.0.1:%s/account;",
                AZURITE.getMappedPort(10000));
    }
}
