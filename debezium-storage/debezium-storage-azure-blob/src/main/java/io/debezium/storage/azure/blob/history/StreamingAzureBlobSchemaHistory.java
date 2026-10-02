/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.azure.blob.history;

import static io.debezium.util.Strings.isNullOrEmpty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlobType;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;

/**
 * Opt-in history that streams an immutable legacy base followed by an append tail.
 * The legacy blob is never rewritten, compacted, or deleted.
 */
public class StreamingAzureBlobSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingAzureBlobSchemaHistory.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int LEASE_SECONDS = 60;
    private static final int MAX_RECORD_BYTES = 4 * 1024 * 1024 - 1;
    private static final int FORMAT_VERSION = 1;

    public static final Field TAIL_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "azure.storage.blob.tail.name")
            .withType(ConfigDef.Type.STRING)
            .withDescription("Append Blob for new schema-history records. Defaults to the legacy blob name plus '.streaming.tail'.");

    public static final Field.Set ALL_FIELDS = AzureBlobSchemaHistory.ALL_FIELDS.with(TAIL_NAME);

    private final DocumentReader reader = DocumentReader.defaultReader();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final List<BlobLeaseClient> leases = new ArrayList<>();
    private final AtomicReference<SchemaHistoryException> ownershipFailure = new AtomicReference<>();
    private BlobContainerClient container;
    private BlobClient base;
    private BlobClient manifest;
    private AppendBlobClient tail;
    private ScheduledExecutorService heartbeat;
    private String tailLeaseId;
    private String baseETag;
    private long baseLength;
    private long tailLength;
    private int tailBlocks;
    private volatile long lastRenewalNanos;
    private boolean running;

    @Override
    public void configure(Configuration configuration, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(configuration, comparator, listener, useCatalogBeforeSchema);
        if (!configuration.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException("Invalid streaming Azure schema-history configuration");
        }
        final String connectionString = configuration.getString(AzureBlobSchemaHistory.ACCOUNT_CONNECTION_STRING);
        final String account = configuration.getString(AzureBlobSchemaHistory.ACCOUNT_NAME);
        final String endpoint = configuration.getString(AzureBlobSchemaHistory.ACCOUNT_BLOB_ENDPOINT);
        if (isNullOrEmpty(connectionString) && !isNullOrEmpty(account) && !isNullOrEmpty(endpoint)) {
            throw new DebeziumException("Configure either account name or blob endpoint, not both");
        }
        final var builder = new BlobServiceClientBuilder();
        if (!isNullOrEmpty(connectionString)) {
            builder.connectionString(connectionString);
        }
        else {
            if (isNullOrEmpty(account) && isNullOrEmpty(endpoint)) {
                throw new DebeziumException("Configure an Azure account connection string, account name, or blob endpoint");
            }
            builder.endpoint(isNullOrEmpty(endpoint) ? String.format("https://%s.blob.core.windows.net", account) : endpoint)
                    .credential(new DefaultAzureCredentialBuilder().build());
        }
        container = builder.buildClient().getBlobContainerClient(configuration.getString(AzureBlobSchemaHistory.CONTAINER_NAME));
        final String baseName = configuration.getString(AzureBlobSchemaHistory.BLOB_NAME);
        final String tailName = configuration.getString(TAIL_NAME, baseName + ".streaming.tail");
        if (baseName.equals(tailName) || (baseName + ".streaming.json").equals(tailName)) {
            throw new DebeziumException("Tail name must differ from the legacy blob and streaming descriptor");
        }
        base = container.getBlobClient(baseName);
        manifest = container.getBlobClient(baseName + ".streaming.json");
        tail = container.getBlobClient(tailName).getAppendBlobClient();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        ownershipFailure.set(null);
        try {
            initializeStorage();
            acquireLease(manifest);
            final BlobProperties manifestProperties = manifest.getProperties();
            if (manifestProperties.getBlobSize() > 4096) {
                throw new SchemaHistoryException("Streaming Azure history descriptor exceeds 4 KiB");
            }
            final var response = manifest.downloadContentWithResponse(null,
                    new BlobRequestConditions().setIfMatch(manifestProperties.getETag()), TIMEOUT, Context.NONE);
            final Document descriptor = reader.read(response.getValue().toString());
            final boolean initializing = descriptor.isEmpty();
            // Creating an empty base for a new history also gives legacy writers a lease guard.
            if (!base.exists()) {
                if (!initializing) {
                    throw new SchemaHistoryException("Streaming history references a missing legacy base");
                }
                base.upload(BinaryData.fromBytes(new byte[0]), false);
            }
            acquireLease(base);
            if (initializing) {
                tail.createIfNotExists();
            }
            else if (!tail.exists()) {
                throw new SchemaHistoryException("Streaming history references a missing append tail");
            }
            tailLeaseId = acquireLease(tail);
            final BlobProperties baseProperties = base.getProperties();
            if (initializing) {
                if (tail.getProperties().getBlobSize() != 0) {
                    throw new SchemaHistoryException("Uninitialized streaming history has a nonempty tail");
                }
                descriptor.setNumber("formatVersion", FORMAT_VERSION);
                descriptor.setString("base", base.getBlobName());
                descriptor.setString("tail", tail.getBlobName());
                descriptor.setString("baseETag", baseProperties.getETag());
                descriptor.setNumber("baseLength", baseProperties.getBlobSize());
                manifest.uploadWithResponse(new BlobParallelUploadOptions(BinaryData.fromString(writer.write(descriptor)))
                        .setRequestConditions(new BlobRequestConditions()
                                .setLeaseId(leases.get(0).getLeaseId())
                                .setIfMatch(response.getDeserializedHeaders().getETag())),
                        TIMEOUT, Context.NONE);
            }
            if (!Integer.valueOf(FORMAT_VERSION).equals(descriptor.getInteger("formatVersion"))
                    || !base.getBlobName().equals(descriptor.getString("base"))
                    || !tail.getBlobName().equals(descriptor.getString("tail"))) {
                throw new SchemaHistoryException("Unsupported or mismatched streaming Azure history descriptor");
            }
            baseETag = descriptor.getString("baseETag");
            final Long length = descriptor.getLong("baseLength");
            if (length == null || !baseProperties.getETag().equals(baseETag) || length != baseProperties.getBlobSize()) {
                throw new SchemaHistoryException("Legacy history changed after streaming layout initialization");
            }
            baseLength = length;
            final BlobProperties tailProperties = tail.getProperties();
            if (tailProperties.getBlobType() != BlobType.APPEND_BLOB) {
                throw new SchemaHistoryException("Streaming Azure history tail must be an Append Blob");
            }
            tailLength = tailProperties.getBlobSize();
            tailBlocks = tailProperties.getCommittedBlockCount();
            if (System.nanoTime() - lastRenewalNanos >= TimeUnit.SECONDS.toNanos(50)) {
                throw new SchemaHistoryException("Azure history lease deadline exceeded during startup");
            }
            heartbeat = Executors.newSingleThreadScheduledExecutor(runnable -> {
                final Thread thread = new Thread(runnable, "debezium-azure-history-lease");
                thread.setDaemon(true);
                return thread;
            });
            heartbeat.scheduleWithFixedDelay(this::renewLeases, 20, 20, TimeUnit.SECONDS);
            running = true;
            super.start();
        }
        catch (IOException | RuntimeException e) {
            running = false;
            closeOwnership();
            throw new SchemaHistoryException("Unable to start streaming Azure schema history", e);
        }
    }

    @Override
    protected synchronized void storeRecord(HistoryRecord record) {
        checkRunning();
        try {
            final byte[] json = writer.write(record.document()).getBytes(StandardCharsets.UTF_8);
            if (json.length > MAX_RECORD_BYTES) {
                throw new SchemaHistoryException("Schema-history record exceeds the 4 MiB atomic append limit");
            }
            if (tailBlocks >= tail.getMaxBlocks()) {
                throw new SchemaHistoryException("Schema-history tail reached Azure's append-block limit; export to a new history before continuing");
            }
            final byte[] payload = new byte[json.length + 1];
            payload[0] = '\n';
            System.arraycopy(json, 0, payload, 1, json.length);
            final long position = tailLength;
            final var conditions = new AppendBlobRequestConditions()
                    .setLeaseId(tailLeaseId)
                    .setAppendPosition(position);
            try {
                tail.appendBlockWithResponse(new ByteArrayInputStream(payload), payload.length, null, conditions, TIMEOUT, Context.NONE);
            }
            catch (RuntimeException e) {
                if (e instanceof BlobStorageException storageException
                        && String.valueOf(storageException.getErrorCode()).startsWith("Lease")) {
                    final var lost = new SchemaHistoryException("Azure history writer lease lost", e);
                    ownershipFailure.compareAndSet(null, lost);
                    throw lost;
                }
                // An SDK retry can return 412 after an append whose success response was lost.
                checkRunning();
                final BlobProperties properties = tail.getProperties();
                if (properties.getBlobSize() != position + payload.length
                        || !Arrays.equals(payload, tail.downloadContentWithResponse(null,
                                new BlobRequestConditions().setIfMatch(properties.getETag()),
                                new BlobRange(position, (long) payload.length), false, TIMEOUT, Context.NONE).getValue().toBytes())) {
                    throw e;
                }
            }
            checkRunning();
            tailLength = position + payload.length;
            ++tailBlocks;
        }
        catch (IOException | RuntimeException e) {
            throw new SchemaHistoryException("Unable to durably append Azure schema history", e);
        }
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> consumer) throws InterruptedException {
        checkRunning();
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Azure history recovery interrupted");
        }
        final Consumer<String> parse = line -> {
            try {
                final HistoryRecord record = new HistoryRecord(reader.read(line));
                if (!record.isValid()) {
                    throw new SchemaHistoryException("History record is missing source or position");
                }
                consumer.accept(record);
            }
            catch (IOException e) {
                throw new SchemaHistoryException("Invalid JSON in Azure schema history", e);
            }
        };
        scan(base, baseLength, baseETag, parse, false);
        final BlobProperties properties = tail.getProperties();
        scan(tail, properties.getBlobSize(), properties.getETag(), parse, false);
    }

    @Override
    public synchronized boolean exists() {
        checkRunning();
        try {
            if (scan(base, baseLength, baseETag, line -> {
            }, true)) {
                return true;
            }
            final BlobProperties properties = tail.getProperties();
            return scan(tail, properties.getBlobSize(), properties.getETag(), line -> {
            }, true);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaHistoryException("Interrupted while checking Azure history", e);
        }
    }

    @Override
    public boolean storageExists() {
        return container.exists();
    }

    @Override
    public void initializeStorage() {
        container.createIfNotExists();
        if (!manifest.exists()) {
            try {
                manifest.upload(BinaryData.fromString("{}"), false);
            }
            catch (BlobStorageException e) {
                if (e.getStatusCode() != 409 && e.getStatusCode() != 412) {
                    throw e;
                }
            }
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        closeOwnership();
        super.stop();
    }

    private boolean scan(BlobClientBase client, long length, String etag, Consumer<String> consumer, boolean firstOnly) throws InterruptedException {
        if (length == 0) {
            return false;
        }
        final var options = new BlobInputStreamOptions().setBlockSize(4 * 1024 * 1024)
                .setRange(new BlobRange(0, length))
                .setRequestConditions(new BlobRequestConditions().setIfMatch(etag));
        boolean found = false;
        try (InputStream stream = client.openInputStream(options);
                ByteArrayOutputStream line = new ByteArrayOutputStream()) {
            final byte[] buffer = new byte[8192];
            int count;
            while ((count = stream.read(buffer)) != -1) {
                checkRunning();
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Azure history recovery interrupted");
                }
                int start = 0;
                for (int i = 0; i < count; ++i) {
                    if (buffer[i] == '\n') {
                        appendLineBytes(line, buffer, start, i - start);
                        if (consumeLine(line, consumer)) {
                            found = true;
                            if (firstOnly) {
                                return true;
                            }
                        }
                        line.reset();
                        start = i + 1;
                    }
                }
                appendLineBytes(line, buffer, start, count - start);
            }
            return consumeLine(line, consumer) || found;
        }
        catch (IOException e) {
            throw new SchemaHistoryException("Unable to stream Azure history blob " + client.getBlobName(), e);
        }
    }

    private void appendLineBytes(ByteArrayOutputStream line, byte[] buffer, int offset, int length) {
        if (length > MAX_RECORD_BYTES - line.size()) {
            throw new SchemaHistoryException("Azure history contains a record larger than 4 MiB; offline remediation is required");
        }
        line.write(buffer, offset, length);
    }

    private boolean consumeLine(ByteArrayOutputStream bytes, Consumer<String> consumer) throws CharacterCodingException {
        if (bytes.size() == 0) {
            return false;
        }
        final String line = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        if (line.isBlank()) {
            return false;
        }
        consumer.accept(line);
        return true;
    }

    private String acquireLease(BlobClientBase client) {
        final var lease = new BlobLeaseClientBuilder().blobClient(client).buildClient();
        final long requested = System.nanoTime();
        lease.acquireLease(LEASE_SECONDS);
        if (leases.isEmpty()) {
            lastRenewalNanos = requested;
        }
        leases.add(lease);
        return lease.getLeaseId();
    }

    private void renewLeases() {
        final long requested = System.nanoTime();
        if (requested - lastRenewalNanos >= TimeUnit.SECONDS.toNanos(50)) {
            ownershipFailure.compareAndSet(null, new SchemaHistoryException("Azure history lease renewal deadline exceeded"));
            return;
        }
        try {
            for (BlobLeaseClient lease : leases) {
                lease.renewLease();
            }
            lastRenewalNanos = requested;
        }
        catch (RuntimeException e) {
            final boolean definitive = e instanceof BlobStorageException
                    && (((BlobStorageException) e).getStatusCode() == 409 || ((BlobStorageException) e).getStatusCode() == 412);
            if (definitive || System.nanoTime() - lastRenewalNanos >= TimeUnit.SECONDS.toNanos(50)) {
                ownershipFailure.compareAndSet(null, new SchemaHistoryException("Azure history writer lease lost", e));
            }
            LOGGER.warn("Unable to renew Azure history leases", e);
        }
    }

    private void checkRunning() {
        final SchemaHistoryException failure = ownershipFailure.get();
        if (failure != null) {
            throw failure;
        }
        if (!running) {
            throw new SchemaHistoryException("Streaming Azure history is not running");
        }
        if (System.nanoTime() - lastRenewalNanos >= TimeUnit.SECONDS.toNanos(50)) {
            final var lost = new SchemaHistoryException("Azure history lease renewal deadline exceeded");
            ownershipFailure.compareAndSet(null, lost);
            throw lost;
        }
    }

    private void closeOwnership() {
        if (heartbeat != null) {
            heartbeat.shutdownNow();
            try {
                if (!heartbeat.awaitTermination(35, TimeUnit.SECONDS)) {
                    LOGGER.warn("Azure history lease heartbeat did not stop before lease release");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            heartbeat = null;
        }
        for (int i = leases.size() - 1; i >= 0; --i) {
            try {
                leases.get(i).releaseLease();
            }
            catch (RuntimeException e) {
                LOGGER.warn("Unable to release Azure history lease; it will expire", e);
            }
        }
        leases.clear();
    }
}
