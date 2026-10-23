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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;

/**
 * Streams an immutable legacy JSONL base followed by an atomic append-only JSONL tail.
 * The legacy blob is never rewritten, compacted, or deleted.
 */
public class StreamingAzureBlobSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingAzureBlobSchemaHistory.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int LEASE_SECONDS = 60;
    private static final String FORMAT_VERSION = "jsonl-1";
    static final int MAX_RECORD_BYTES = 4 * 1024 * 1024 - 1;
    private static final String BASE_FORMAT = "dbzhistoryformat";
    private static final String BASE_TAIL = "dbzhistorytail";
    private static final String BASE_LAYOUT = "dbzhistorylayoutid";

    public static final Field TAIL_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "azure.storage.blob.tail.name")
            .withType(ConfigDef.Type.STRING)
            .withDescription("Append Blob for new schema-history records. Defaults to the legacy blob name plus '.streaming.tail'.");

    public static final Field.Set ALL_FIELDS = AzureBlobSchemaHistory.ALL_FIELDS.with(TAIL_NAME);

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final List<BlobLeaseClient> leases = new ArrayList<>();
    private final AtomicReference<SchemaHistoryException> ownershipFailure = new AtomicReference<>();
    private BlobContainerClient container;
    private BlobClient base;
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
        if (baseName.equals(tailName)) {
            throw new DebeziumException("Tail name must differ from the legacy blob");
        }
        base = container.getBlobClient(baseName);
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
            // Creating an empty base for a new history also gives legacy writers a lease guard.
            if (!base.exists()) {
                if (tail.exists()) {
                    throw new SchemaHistoryException("Streaming history references a missing legacy base");
                }
                try {
                    base.upload(BinaryData.fromBytes(new byte[0]), false);
                }
                catch (BlobStorageException e) {
                    if (e.getStatusCode() != 409 && e.getStatusCode() != 412) {
                        throw e;
                    }
                }
            }
            final String baseLeaseId = acquireLease(base);
            BlobProperties baseProperties = base.getProperties();
            final Map<String, String> baseMetadata = normalizedMetadata(baseProperties);
            final boolean marked = baseMetadata.containsKey(BASE_FORMAT) || baseMetadata.containsKey(BASE_TAIL) || baseMetadata.containsKey(BASE_LAYOUT);
            final String layoutId;
            if (marked) {
                if (!FORMAT_VERSION.equals(baseMetadata.get(BASE_FORMAT))
                        || !tail.getBlobName().equals(baseMetadata.get(BASE_TAIL))) {
                    throw new SchemaHistoryException("Unsupported base layout marker or configured tail name changed; repair required");
                }
                layoutId = baseMetadata.get(BASE_LAYOUT);
                validateLayoutId(layoutId);
                if (!tail.exists()) {
                    throw new SchemaHistoryException("Initialized history tail is missing; history loss requires repair");
                }
            }
            else {
                layoutId = UUID.randomUUID().toString();
                tail.createIfNotExists();
            }
            tailLeaseId = acquireLease(tail);
            final BlobProperties tailProperties = tail.getProperties();
            if (tailProperties.getBlobType() != BlobType.APPEND_BLOB) {
                throw new SchemaHistoryException("Streaming Azure history tail must be an Append Blob");
            }
            if (!marked) {
                if (tailProperties.getBlobSize() != 0 || !tailProperties.getMetadata().isEmpty()) {
                    throw new SchemaHistoryException("Unmarked base has an initialized or nonempty tail; repair required");
                }
                final Map<String, String> marker = new java.util.HashMap<>(baseMetadata);
                marker.put(BASE_FORMAT, FORMAT_VERSION);
                marker.put(BASE_TAIL, tail.getBlobName());
                marker.put(BASE_LAYOUT, layoutId);
                base.setMetadataWithResponse(marker, new BlobRequestConditions().setLeaseId(baseLeaseId)
                        .setIfMatch(baseProperties.getETag()), TIMEOUT, Context.NONE);
                baseProperties = base.getProperties();
            }
            initializeTailMetadata(tailProperties, baseProperties, layoutId);
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
            scan(tail, tailLength, tail.getProperties().getETag(), record -> {
            }, false);
            super.start();
        }
        catch (InterruptedException | RuntimeException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            running = false;
            closeOwnership();
            throw new SchemaHistoryException("Unable to start streaming Azure schema history", e);
        }
    }

    @Override
    protected synchronized void storeRecord(HistoryRecord record) {
        checkRunning();
        try {
            final byte[] json = serialize(record);
            if (tailBlocks >= tail.getMaxBlocks()) {
                throw new SchemaHistoryException("Schema-history tail reached Azure's append-block limit; export to a new history before continuing");
            }
            final byte[] payload = Arrays.copyOf(json, json.length + 1);
            payload[json.length] = '\n';
            appendRecord(payload);
        }
        catch (IOException | RuntimeException e) {
            ownershipFailure.compareAndSet(null, new SchemaHistoryException("History append failed; restart validates the tail before admitting writes", e));
            throw new SchemaHistoryException("Unable to durably append Azure schema history", e);
        }
    }

    private byte[] serialize(HistoryRecord record) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            writer.write(record.document(), new OutputStream() {
                @Override
                public void write(int value) throws IOException {
                    reserve(1);
                    bytes.write(value);
                }

                @Override
                public void write(byte[] values, int offset, int length) throws IOException {
                    reserve(length);
                    bytes.write(values, offset, length);
                }

                private void reserve(int length) {
                    if (length > MAX_RECORD_BYTES - bytes.size()) {
                        throw oversizedRecord();
                    }
                }
            });
            return bytes.toByteArray();
        }
    }

    private SchemaHistoryException oversizedRecord() {
        return new SchemaHistoryException("Azure schema-history record exceeds " + MAX_RECORD_BYTES
                + " UTF-8 bytes (4 MiB append block minus newline); reduce the record or export/remediate history before restarting");
    }

    private void appendRecord(byte[] payload) {
        checkRunning();
        final long position = tailLength;
        final var conditions = new AppendBlobRequestConditions().setLeaseId(tailLeaseId).setAppendPosition(position);
        try {
            appendBlock(payload, conditions);
        }
        catch (RuntimeException e) {
            if (e instanceof BlobStorageException storageException
                    && String.valueOf(storageException.getErrorCode()).startsWith("Lease")) {
                throw new SchemaHistoryException("Azure history writer lease lost", e);
            }
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

    protected void appendBlock(byte[] payload, AppendBlobRequestConditions conditions) {
        tail.appendBlockWithResponse(new ByteArrayInputStream(payload), payload.length, null, conditions, TIMEOUT, Context.NONE);
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> consumer) throws InterruptedException {
        checkRunning();
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Azure history recovery interrupted");
        }
        try {
            scan(base, baseLength, baseETag, consumer, false);
            final BlobProperties properties = tail.getProperties();
            scan(tail, properties.getBlobSize(), properties.getETag(), consumer, false);
        }
        catch (SchemaHistoryException e) {
            if (Thread.currentThread().isInterrupted()) {
                final var interrupted = new InterruptedException("Azure history recovery interrupted");
                interrupted.initCause(e);
                throw interrupted;
            }
            throw e;
        }
    }

    @Override
    public synchronized boolean exists() {
        checkRunning();
        try {
            if (scan(base, baseLength, baseETag, line -> {
            }, true)) {
                return true;
            }
            return tailLength > 0;
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
    }

    private Map<String, String> normalizedMetadata(BlobProperties properties) {
        final Map<String, String> metadata = new java.util.HashMap<>();
        properties.getMetadata().forEach((key, value) -> {
            if (metadata.put(key.toLowerCase(java.util.Locale.ROOT), value) != null) {
                throw new SchemaHistoryException("Duplicate history metadata key");
            }
        });
        return metadata;
    }

    private void validateLayoutId(String layoutId) {
        try {
            if (layoutId == null || !UUID.fromString(layoutId).toString().equals(layoutId)) {
                throw new IllegalArgumentException("Missing or noncanonical layout ID");
            }
        }
        catch (IllegalArgumentException e) {
            throw new SchemaHistoryException("Malformed base layout ID; repair required", e);
        }
    }

    private void initializeTailMetadata(BlobProperties tailProperties, BlobProperties baseProperties, String layoutId) {
        final Map<String, String> metadata = normalizedMetadata(tailProperties);
        if (!metadata.keySet().stream().allMatch(key -> key.equals("formatversion") || key.equals("baseetag") || key.equals("baselength") || key.equals("layoutid"))
                || (metadata.containsKey("formatversion") && !FORMAT_VERSION.equals(metadata.get("formatversion")))) {
            throw new SchemaHistoryException("Unsupported JSONL history tail metadata; repair required");
        }
        if (metadata.containsKey("layoutid") && !layoutId.equals(metadata.get("layoutid"))) {
            throw new SchemaHistoryException("Base and tail layout IDs mismatch; repair required");
        }
        final String etag = metadata.get("baseetag");
        final String lengthText = metadata.get("baselength");
        final Long length;
        try {
            length = lengthText == null ? null : Long.parseLong(lengthText);
        }
        catch (NumberFormatException e) {
            throw new SchemaHistoryException("Malformed JSONL history baseLength metadata", e);
        }
        if ((etag != null && !baseProperties.getETag().equals(etag))
                || (length != null && length != baseProperties.getBlobSize())) {
            throw new SchemaHistoryException("Legacy history changed after streaming layout initialization");
        }
        if (metadata.size() != 4) {
            if (tailProperties.getBlobSize() != 0) {
                throw new SchemaHistoryException("Nonempty JSONL tail has incomplete metadata; repair required");
            }
            final Map<String, String> initialized = Map.of("formatVersion", FORMAT_VERSION, "layoutId", layoutId,
                    "baseETag", baseProperties.getETag(), "baseLength", Long.toString(baseProperties.getBlobSize()));
            tail.setMetadataWithResponse(initialized, new BlobRequestConditions().setLeaseId(tailLeaseId)
                    .setIfMatch(tailProperties.getETag()), TIMEOUT, Context.NONE);
        }
        baseETag = baseProperties.getETag();
        baseLength = baseProperties.getBlobSize();
    }

    @Override
    public synchronized void stop() {
        running = false;
        closeOwnership();
        super.stop();
    }

    private boolean scan(BlobClientBase client, long length, String etag, Consumer<HistoryRecord> consumer, boolean firstOnly) throws InterruptedException {
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
            boolean nonblank = false;
            int count;
            while ((count = stream.read(buffer)) != -1) {
                checkRunning();
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Azure history recovery interrupted");
                }
                int start = 0;
                for (int i = 0; i < count; ++i) {
                    if (buffer[i] != '\n' && buffer[i] != '\r' && buffer[i] != ' ' && buffer[i] != '\t') {
                        nonblank = true;
                        if (firstOnly) {
                            return true;
                        }
                    }
                    if (buffer[i] == '\n') {
                        appendLine(line, buffer, start, i - start);
                        if (nonblank) {
                            consumeLine(line, consumer);
                            found = true;
                        }
                        line.reset();
                        nonblank = false;
                        start = i + 1;
                    }
                }
                appendLine(line, buffer, start, count - start);
            }
            if (nonblank) {
                if (client == tail) {
                    throw new SchemaHistoryException("Truncated JSONL tail: final atomic record is missing its newline; repair required");
                }
                consumeLine(line, consumer);
                found = true;
            }
            return found;
        }
        catch (IOException e) {
            throw new SchemaHistoryException("Unable to stream Azure history blob " + client.getBlobName(), e);
        }
    }

    private void appendLine(ByteArrayOutputStream line, byte[] bytes, int offset, int length) {
        if (length > MAX_RECORD_BYTES - line.size()) {
            throw oversizedRecord();
        }
        line.write(bytes, offset, length);
    }

    private void consumeLine(ByteArrayOutputStream line, Consumer<HistoryRecord> consumer) throws IOException {
        final String json = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(line.toByteArray())).toString();
        final HistoryRecord record;
        try {
            try (var parser = new com.fasterxml.jackson.core.JsonFactory().createParser(json)) {
                if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                    throw new IOException("History line must be a JSON object");
                }
                parser.skipChildren();
                if (parser.nextToken() != null) {
                    throw new IOException("Trailing content after history JSON object");
                }
            }
            record = new HistoryRecord(reader.read(json));
        }
        catch (IOException | RuntimeException e) {
            throw new SchemaHistoryException("Invalid JSON in Azure schema history", e);
        }
        if (!record.isValid()) {
            throw new SchemaHistoryException("History record is missing source or position");
        }
        consumer.accept(record);
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
