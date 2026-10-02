/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static io.debezium.util.Strings.isNullOrEmpty;
import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.document.Value;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;

/**
 * Experimental schema history implementation used to validate a checkpoint and
 * delta layout on Azure Blob Storage.
 * <p>
 * The configured legacy blob is never modified. A small manifest references one
 * active checkpoint, immutable streaming delta blobs, and at most one pending
 * checkpoint. Recovery streams only the referenced blobs.
 * </p>
 * <p>
 * This class is a proof of concept. In particular, promotion occurs when the
 * snapshot schema buffering scope completes. Production use also requires
 * correlation with durable source offset commits.
 * </p>
 */
public class VersionedAzureBlobSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(VersionedAzureBlobSchemaHistory.class);

    private static final String ENDPOINT_FORMAT = "https://%s.blob.core.windows.net";
    private static final String FORMAT_VERSION = "1";
    private static final int MANIFEST_UPDATE_ATTEMPTS = 5;

    public static final Field ACCOUNT_CONNECTION_STRING = AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING;
    public static final Field ACCOUNT_NAME = AzureBlobSchemaHistory.ACCOUNT_NAME;
    public static final Field ACCOUNT_BLOB_ENDPOINT = AzureBlobSchemaHistory.ACCOUNT_BLOB_ENDPOINT;
    public static final Field CONTAINER_NAME = AzureBlobSchemaHistory.CONTAINER_NAME;
    public static final Field BLOB_NAME = AzureBlobSchemaHistory.BLOB_NAME;

    public static final Field POC_LAYOUT_SUFFIX = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "azure.storage.poc.layout.suffix")
            .withDisplayName("Versioned schema history POC layout suffix")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(".versioned-poc");

    public static final Field.Set ALL_FIELDS = Field.setOf(
            ACCOUNT_CONNECTION_STRING,
            ACCOUNT_NAME,
            ACCOUNT_BLOB_ENDPOINT,
            CONTAINER_NAME,
            BLOB_NAME,
            POC_LAYOUT_SUFFIX);

    private final AtomicBoolean running = new AtomicBoolean();
    private final DocumentReader documentReader = DocumentReader.defaultReader();
    private final DocumentWriter documentWriter = DocumentWriter.defaultWriter();

    private BlobServiceClient blobServiceClient;
    private BlobContainerClient containerClient;
    private BlobClient legacyBlobClient;
    private BlobClient manifestBlobClient;

    private String container;
    private String blobName;
    private String layoutPrefix;

    private PendingCheckpoint pendingCheckpoint;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        container = config.getString(CONTAINER_NAME);
        blobName = config.getString(BLOB_NAME);
        layoutPrefix = blobName + config.getString(POC_LAYOUT_SUFFIX);

        if (config.getBoolean(INTERNAL_PREFER_DDL)) {
            throw new DebeziumException(getClass().getSimpleName() + " requires logical table changes and does not support prefer.ddl=true");
        }

        if (isNullOrEmpty(config.getString(ACCOUNT_CONNECTION_STRING))
                && !isNullOrEmpty(config.getString(ACCOUNT_BLOB_ENDPOINT))
                && !isNullOrEmpty(config.getString(ACCOUNT_NAME))) {
            throw new DebeziumException(
                    "Only one of '" + ACCOUNT_BLOB_ENDPOINT.name() + "' or '" + ACCOUNT_NAME.name()
                            + "' should be set, but not both.");
        }
    }

    @Override
    public synchronized void start() {
        initializeClients();
        if (!containerClient.exists()) {
            initializeStorage();
        }
        running.set(true);
        super.start();
    }

    @Override
    public synchronized void stop() {
        running.set(false);
        pendingCheckpoint = null;
        super.stop();
    }

    @Override
    public synchronized void startBuffering() {
        ensureRunning();
        final Manifest base = ensureManifest().manifest;
        final String pendingBlob = checkpointBlobName("pending-" + UUID.randomUUID());
        final PendingCheckpoint pending = new PendingCheckpoint(pendingBlob, base);

        if (base.activeCheckpoint != null) {
            streamRecords(blobClient(base.activeCheckpoint), pending::apply);
        }
        base.deltaBlobs.forEach(delta -> streamRecords(blobClient(delta), pending::apply));

        writeBlob(pendingBlob, pending.serialize(), true);
        updateManifest(manifest -> {
            manifest.pendingCheckpoint = pendingBlob;
            return manifest;
        });
        pendingCheckpoint = pending;
        LOGGER.info("Started pending schema checkpoint '{}'", pendingBlob);
    }

    @Override
    public synchronized void stopBuffering() {
        stopBuffering(true);
    }

    @Override
    public synchronized void stopBuffering(boolean commit) {
        if (pendingCheckpoint == null) {
            return;
        }

        final PendingCheckpoint completed = pendingCheckpoint;
        pendingCheckpoint = null;

        if (commit) {
            writeBlob(completed.blobName, completed.serialize(), true);
            updateManifest(manifest -> {
                if (!completed.blobName.equals(manifest.pendingCheckpoint)) {
                    throw new SchemaHistoryException("Pending checkpoint changed before it could be committed");
                }
                if (!Objects.equals(completed.baseActiveCheckpoint, manifest.activeCheckpoint)
                        || !completed.baseDeltaBlobs.equals(manifest.deltaBlobs)) {
                    throw new SchemaHistoryException("Active schema history changed while the pending checkpoint was being built");
                }
                manifest.activeCheckpoint = completed.blobName;
                manifest.pendingCheckpoint = null;
                manifest.deltaBlobs.clear();
                return manifest;
            });
            LOGGER.info("Promoted pending schema checkpoint '{}' with {} table record(s) and {} context record(s)",
                    completed.blobName, completed.tableRecords.size(), completed.contextRecords.size());
        }
        else {
            updateManifest(manifest -> {
                if (completed.blobName.equals(manifest.pendingCheckpoint)) {
                    manifest.pendingCheckpoint = null;
                }
                return manifest;
            });
            LOGGER.info("Aborted pending schema checkpoint '{}'", completed.blobName);
        }
    }

    @Override
    protected synchronized void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        ensureRunning();
        if (pendingCheckpoint != null) {
            pendingCheckpoint.apply(record);
            writeBlob(pendingCheckpoint.blobName, pendingCheckpoint.serialize(), true);
            return;
        }

        ensureManifest();
        for (int attempt = 0; attempt < MANIFEST_UPDATE_ATTEMPTS; ++attempt) {
            final ManifestWithEtag current = readManifest();
            final long sequence = current.manifest.nextSequence;
            final String deltaBlob = deltaBlobName(sequence, UUID.randomUUID());
            writeBlob(deltaBlob, serializeRecord(record), false);

            try {
                final Manifest next = current.manifest.copy();
                next.deltaBlobs.add(deltaBlob);
                next.nextSequence = sequence + 1;
                writeManifest(next, current.etag);
                return;
            }
            catch (BlobStorageException e) {
                if (e.getStatusCode() != 412) {
                    throw new SchemaHistoryException("Unable to commit schema history delta", e);
                }
                LOGGER.debug("Manifest changed while committing delta '{}'; retrying", deltaBlob);
            }
        }

        throw new SchemaHistoryException("Unable to commit schema history delta after " + MANIFEST_UPDATE_ATTEMPTS + " attempts");
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> consumer) {
        ensureRunning();
        if (!manifestBlobClient.exists()) {
            if (legacyBlobClient.exists()) {
                streamRecords(legacyBlobClient, consumer);
            }
            return;
        }

        final Manifest manifest = readManifest().manifest;
        if (manifest.activeCheckpoint != null) {
            streamRecords(blobClient(manifest.activeCheckpoint), consumer);
        }
        for (String deltaBlob : manifest.deltaBlobs) {
            streamRecords(blobClient(deltaBlob), consumer);
        }
    }

    @Override
    public synchronized boolean exists() {
        initializeClients();
        if (manifestBlobClient.exists()) {
            final Manifest manifest = readManifest().manifest;
            return manifest.activeCheckpoint != null || !manifest.deltaBlobs.isEmpty();
        }
        return legacyBlobClient.exists() && legacyBlobClient.getProperties().getBlobSize() > 0;
    }

    @Override
    public synchronized boolean storageExists() {
        initializeClients();
        return containerClient.exists();
    }

    @Override
    public synchronized void initializeStorage() {
        initializeClients();
        if (!containerClient.exists()) {
            blobServiceClient.createBlobContainer(container);
        }
    }

    /**
     * Creates the first compact checkpoint from the configured legacy blob.
     * <p>
     * The caller must stop the connector and ensure the legacy blob contains
     * records only through the durable connector offset.
     * </p>
     *
     * @return {@code true} if migration created a manifest, or {@code false} when
     *         there was nothing to migrate or the layout already existed
     */
    public synchronized boolean migrateLegacyHistory() {
        ensureRunning();
        if (manifestBlobClient.exists() || !legacyBlobClient.exists()) {
            return false;
        }

        final PendingCheckpoint migrated = new PendingCheckpoint(checkpointBlobName("migrated-" + UUID.randomUUID()));
        streamRecords(legacyBlobClient, migrated::apply);
        writeBlob(migrated.blobName, migrated.serialize(), false);

        final Manifest manifest = new Manifest();
        manifest.activeCheckpoint = migrated.blobName;
        writeManifest(manifest, null);
        LOGGER.info("Migrated legacy schema history '{}' into compact checkpoint '{}' with {} table record(s)",
                blobName, migrated.blobName, migrated.tableRecords.size());
        return true;
    }

    /**
     * Deletes unreferenced POC blobs. The configured legacy blob is never deleted.
     *
     * @return number of deleted blobs
     */
    public synchronized int cleanupOrphanBlobs() {
        ensureRunning();
        final Set<String> referenced = new HashSet<>();
        referenced.add(manifestBlobName());

        if (manifestBlobClient.exists()) {
            final Manifest manifest = readManifest().manifest;
            if (manifest.activeCheckpoint != null) {
                referenced.add(manifest.activeCheckpoint);
            }
            if (manifest.pendingCheckpoint != null) {
                referenced.add(manifest.pendingCheckpoint);
            }
            referenced.addAll(manifest.deltaBlobs);
        }

        int deleted = 0;
        final ListBlobsOptions options = new ListBlobsOptions().setPrefix(layoutPrefix + "/");
        for (BlobItem item : containerClient.listBlobs(options, null)) {
            if (!referenced.contains(item.getName()) && blobClient(item.getName()).deleteIfExists()) {
                ++deleted;
            }
        }
        return deleted;
    }

    @Override
    public String toString() {
        return "Versioned Azure Blob Storage POC";
    }

    private void initializeClients() {
        if (blobServiceClient == null) {
            if (isNullOrEmpty(config.getString(ACCOUNT_CONNECTION_STRING))) {
                String endpoint = config.getString(ACCOUNT_BLOB_ENDPOINT);
                if (isNullOrEmpty(endpoint)) {
                    endpoint = format(ENDPOINT_FORMAT, config.getString(ACCOUNT_NAME));
                }
                blobServiceClient = new BlobServiceClientBuilder()
                        .endpoint(endpoint)
                        .credential(new DefaultAzureCredentialBuilder().build())
                        .buildClient();
            }
            else {
                blobServiceClient = new BlobServiceClientBuilder()
                        .connectionString(config.getString(ACCOUNT_CONNECTION_STRING))
                        .buildClient();
            }
        }

        if (containerClient == null) {
            containerClient = blobServiceClient.getBlobContainerClient(container);
            legacyBlobClient = containerClient.getBlobClient(blobName);
            manifestBlobClient = containerClient.getBlobClient(manifestBlobName());
        }
    }

    private ManifestWithEtag ensureManifest() {
        if (manifestBlobClient.exists()) {
            return readManifest();
        }

        if (legacyBlobClient.exists()) {
            migrateLegacyHistory();
            return readManifest();
        }

        final Manifest manifest = new Manifest();
        writeManifest(manifest, null);
        return readManifest();
    }

    private ManifestWithEtag readManifest() {
        try {
            final BlobDownloadContentResponse response = manifestBlobClient.downloadContentWithResponse(null, null, null, Context.NONE);
            final Document document = documentReader.read(response.getValue().toString());
            return new ManifestWithEtag(Manifest.fromDocument(document), response.getDeserializedHeaders().getETag());
        }
        catch (IOException e) {
            throw new SchemaHistoryException("Unable to parse versioned schema history manifest", e);
        }
    }

    private void updateManifest(UnaryOperator<Manifest> update) {
        for (int attempt = 0; attempt < MANIFEST_UPDATE_ATTEMPTS; ++attempt) {
            final ManifestWithEtag current = manifestBlobClient.exists()
                    ? readManifest()
                    : new ManifestWithEtag(new Manifest(), null);
            final Manifest next = update.apply(current.manifest.copy());
            try {
                writeManifest(next, current.etag);
                return;
            }
            catch (BlobStorageException e) {
                if (e.getStatusCode() != 412 && !(current.etag == null && e.getStatusCode() == 409)) {
                    throw new SchemaHistoryException("Unable to update versioned schema history manifest", e);
                }
            }
        }
        throw new SchemaHistoryException("Unable to update versioned schema history manifest after "
                + MANIFEST_UPDATE_ATTEMPTS + " attempts");
    }

    private void writeManifest(Manifest manifest, String expectedEtag) {
        final BlobRequestConditions conditions = new BlobRequestConditions();
        if (expectedEtag == null) {
            conditions.setIfNoneMatch("*");
        }
        else {
            conditions.setIfMatch(expectedEtag);
        }

        final BinaryData data = BinaryData.fromString(serializeDocument(manifest.toDocument()));
        final BlobParallelUploadOptions options = new BlobParallelUploadOptions(data)
                .setRequestConditions(conditions);
        manifestBlobClient.uploadWithResponse(options, null, Context.NONE);
    }

    private void streamRecords(BlobClient client, Consumer<HistoryRecord> consumer) {
        try (InputStream stream = client.openInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    consumer.accept(new HistoryRecord(documentReader.read(line)));
                }
            }
        }
        catch (IOException e) {
            throw new SchemaHistoryException("Unable to read schema history blob '" + client.getBlobName() + "'", e);
        }
    }

    private void writeBlob(String name, String content, boolean overwrite) {
        blobClient(name).upload(BinaryData.fromString(content), overwrite);
    }

    private BlobClient blobClient(String name) {
        return containerClient.getBlobClient(name);
    }

    private String serializeRecord(HistoryRecord record) {
        return serializeDocument(record.document()) + System.lineSeparator();
    }

    private String serializeDocument(Document document) {
        try {
            return documentWriter.write(document);
        }
        catch (IOException e) {
            throw new SchemaHistoryException("Unable to serialize schema history document", e);
        }
    }

    private String manifestBlobName() {
        return layoutPrefix + "/manifest.json";
    }

    private String checkpointBlobName(String id) {
        return layoutPrefix + "/checkpoints/" + id + ".jsonl";
    }

    private String deltaBlobName(long sequence, UUID id) {
        return layoutPrefix + "/deltas/" + String.format("%020d-%s.jsonl", sequence, id);
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new SchemaHistoryException("The schema history has been stopped");
        }
    }

    private final class PendingCheckpoint {
        private final String blobName;
        private final String baseActiveCheckpoint;
        private final List<String> baseDeltaBlobs;
        private final Map<String, String> tableRecords = new TreeMap<>();
        private final Map<String, String> contextRecords = new LinkedHashMap<>();

        private PendingCheckpoint(String blobName) {
            this(blobName, new Manifest());
        }

        private PendingCheckpoint(String blobName, Manifest base) {
            this.blobName = blobName;
            this.baseActiveCheckpoint = base.activeCheckpoint;
            this.baseDeltaBlobs = List.copyOf(base.deltaBlobs);
        }

        private void apply(HistoryRecord record) {
            final String serialized = serializeDocument(record.document());
            final Array changes = record.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            if (changes == null || changes.isEmpty()) {
                final Document normalized = record.document().clone();
                normalized.remove(HistoryRecord.Fields.TIMESTAMP);
                contextRecords.put(digest(serializeDocument(normalized)), serialized);
                return;
            }

            for (Value value : changes.streamValues().toList()) {
                final Document change = value.asDocument();
                final String type = change.getString("type");
                final String id = change.getString("id");
                final String previousId = change.getString("previousId");

                if (previousId != null) {
                    tableRecords.remove(previousId);
                }
                if ("DROP".equals(type)) {
                    tableRecords.remove(id);
                }
                else {
                    final Document tableRecord = record.document().clone();
                    tableRecord.setArray(HistoryRecord.Fields.TABLE_CHANGES, Array.create((Object) change.clone()));
                    tableRecords.put(id, serializeDocument(tableRecord));
                }
            }
        }

        private String serialize() {
            final StringBuilder result = new StringBuilder();
            final Set<String> emitted = new LinkedHashSet<>();
            contextRecords.values().forEach(record -> appendUnique(result, emitted, record));
            tableRecords.values().forEach(record -> appendUnique(result, emitted, record));
            return result.toString();
        }

        private void appendUnique(StringBuilder target, Set<String> emitted, String record) {
            if (emitted.add(record)) {
                target.append(record).append(System.lineSeparator());
            }
        }
    }

    private static String digest(String value) {
        try {
            final byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static final class Manifest {
        private String activeCheckpoint;
        private String pendingCheckpoint;
        private final List<String> deltaBlobs = new ArrayList<>();
        private long nextSequence;

        private Manifest copy() {
            final Manifest copy = new Manifest();
            copy.activeCheckpoint = activeCheckpoint;
            copy.pendingCheckpoint = pendingCheckpoint;
            copy.deltaBlobs.addAll(deltaBlobs);
            copy.nextSequence = nextSequence;
            return copy;
        }

        private Document toDocument() {
            final Document document = Document.create();
            document.setString("formatVersion", FORMAT_VERSION);
            document.setString("activeCheckpoint", activeCheckpoint);
            document.setString("pendingCheckpoint", pendingCheckpoint);
            document.setNumber("nextSequence", nextSequence);
            document.setArray("deltaBlobs", deltaBlobs.toArray());
            return document;
        }

        private static Manifest fromDocument(Document document) {
            final Manifest manifest = new Manifest();
            manifest.activeCheckpoint = document.getString("activeCheckpoint");
            manifest.pendingCheckpoint = document.getString("pendingCheckpoint");
            final Long nextSequence = document.getLong("nextSequence");
            manifest.nextSequence = nextSequence == null ? 0 : nextSequence;
            final Array deltas = document.getArray("deltaBlobs");
            if (deltas != null) {
                deltas.streamValues().map(Value::asString).forEach(manifest.deltaBlobs::add);
            }
            return manifest;
        }
    }

    private record ManifestWithEtag(Manifest manifest, String etag) {
    }
}
