/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.spi.storage.DefaultOffsetStorageReader;
import io.debezium.spi.storage.DefaultOffsetStorageWriter;
import io.debezium.spi.storage.OffsetStorageReader;
import io.debezium.spi.storage.OffsetStorageWriter;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.storage.nats.NatsConnection;
import io.nats.client.ObjectStore;
import io.nats.client.api.ObjectInfo;

/**
 * Implementation of OffsetStore that saves to NATS Object Store.
 * <p>
 * Each offset is stored as an individual object in the bucket, named by the
 * base64url-encoded offset key. This keeps writes proportional to the number
 * of changed offsets and lets multiple connectors share a bucket without
 * clobbering each other's offsets. A zero-byte object represents a null
 * offset value.
 * <p>
 * Keys whose encoded name would exceed the NATS object name limit (255
 * characters) are stored under a SHA-256 hash of the key, with the original
 * key embedded in the object payload.
 *
 * @author Nick Chomey
 */
public class NatsOffsetBackingStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsOffsetBackingStore.class);
    private static final Base64.Encoder OBJECT_NAME_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder OBJECT_NAME_DECODER = Base64.getUrlDecoder();

    /**
     * Documented NATS object name limit. The prefix below is not part of the
     * base64url alphabet, so names are unambiguous.
     */
    private static final int MAX_OBJECT_NAME_LENGTH = 255;
    private static final String LONG_KEY_PREFIX = "long:";

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor;

    private NatsOffsetBackingStoreConfig config;
    private NatsConnection natsConnection;
    private ObjectStore objectStore;

    private void connect() {
        try {
            natsConnection = NatsConnection.getInstance(config, config.instanceScope());
            objectStore = natsConnection.getOrCreateObjectStore(config.getBucketName());
            LOGGER.info("Connected to NATS Object Store bucket: {}", config.getBucketName());
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to connect to NATS Object Store", e);
        }
    }

    @Override
    public void configure(Configuration config) {
        this.config = new NatsOffsetBackingStoreConfig(config);
    }

    @VisibleForTesting
    public void configure(NatsOffsetBackingStoreConfig config) {
        this.config = config;
    }

    @Override
    public synchronized void start() {
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nats-offset-backing-store");
            t.setDaemon(true);
            return t;
        });
        LOGGER.info("Starting NatsOffsetBackingStore");
        connect();
        load();
    }

    @VisibleForTesting
    synchronized void startNoLoad() {
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nats-offset-backing-store");
            t.setDaemon(true);
            return t;
        });
        connect();
    }

    @Override
    public synchronized void stop() {
        LOGGER.info("Stopping NatsOffsetBackingStore");
        if (executor != null) {
            executor.shutdown();
            try {
                // Give in-flight offset writes a chance to complete before
                // closing the connection underneath them
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
        if (natsConnection != null) {
            natsConnection.close();
        }
    }

    /**
     * Load offsets from NATS Object Store
     */
    @VisibleForTesting
    void load() {
        try {
            LOGGER.debug("Loading offsets from NATS Object Store bucket: {}", config.getBucketName());
            data.clear();

            List<ObjectInfo> objects = objectStore.getList();
            for (ObjectInfo info : objects) {
                String objectName = info.getObjectName();
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    objectStore.get(objectName, baos);
                    byte[] payload = baos.toByteArray();
                    if (objectName.startsWith(LONG_KEY_PREFIX)) {
                        // Payload layout: 4-byte key length + key + value
                        ByteBuffer buf = ByteBuffer.wrap(payload);
                        int keyLength = buf.getInt();
                        byte[] keyBytes = new byte[keyLength];
                        buf.get(keyBytes);
                        byte[] valueBytes = new byte[buf.remaining()];
                        buf.get(valueBytes);
                        data.put(ByteBuffer.wrap(keyBytes),
                                valueBytes.length == 0 ? null : ByteBuffer.wrap(valueBytes));
                    }
                    else {
                        ByteBuffer key = ByteBuffer.wrap(OBJECT_NAME_DECODER.decode(objectName));
                        // A zero-byte object is a tombstone for a null value
                        ByteBuffer value = payload.length == 0 ? null : ByteBuffer.wrap(payload);
                        data.put(key, value);
                    }
                }
                catch (Exception e) {
                    // Object may have been deleted concurrently; skip it
                    LOGGER.debug("Failed to load object '{}' from NATS Object Store: {}", objectName,
                            e.toString());
                }
            }

            LOGGER.info("Loaded {} offsets from NATS Object Store", data.size());
        }
        catch (Exception e) {
            LOGGER.error("Failed to load offsets from NATS Object Store", e);
            throw new DebeziumException("Failed to load offsets", e);
        }
    }

    /**
     * Save offsets to NATS Object Store
     */
    protected void save() {
        save(data.keySet());
    }

    /**
     * Save the given offset keys to NATS Object Store, one object per key.
     * A null value is stored as a zero-byte object (tombstone).
     */
    private void save(Collection<ByteBuffer> keys) {
        try {
            executeWithRetry(() -> {
                for (ByteBuffer key : keys) {
                    if (key == null) {
                        continue;
                    }
                    byte[] keyBytes = toByteArray(key);
                    String objectName = objectNameFor(keyBytes);
                    byte[] valueBytes = data.get(key) != null ? toByteArray(data.get(key)) : new byte[0];
                    byte[] payload = objectName.startsWith(LONG_KEY_PREFIX)
                            ? payloadWithKey(keyBytes, valueBytes)
                            : valueBytes;
                    try {
                        objectStore.put(objectName, payload);
                        LOGGER.trace("Stored offset object '{}' ({} bytes) in NATS Object Store",
                                objectName, payload.length);
                    }
                    catch (Exception putEx) {
                        // The bucket (or its backing stream) may have been lost,
                        // e.g. after a server restart. Probe and (re)create it,
                        // then let the retry loop attempt the put again.
                        LOGGER.debug("ObjectStore put failed, probing bucket '{}': {}", config.getBucketName(),
                                putEx.toString());
                        objectStore = natsConnection.getOrCreateObjectStore(config.getBucketName());
                        throw putEx;
                    }
                }
            });

            LOGGER.debug("Successfully saved {} offsets to NATS Object Store", keys.size());
        }
        catch (Exception e) {
            LOGGER.error("Failed to save offsets to NATS Object Store", e);
            throw new DebeziumException("Failed to save offsets", e);
        }
    }

    /**
     * Compute the object name for an offset key: the base64url encoding, or a
     * SHA-256 hash with the {@link #LONG_KEY_PREFIX} prefix when the encoding
     * would exceed the NATS object name limit.
     */
    private static String objectNameFor(byte[] keyBytes) {
        String name = OBJECT_NAME_ENCODER.encodeToString(keyBytes);
        if (name.length() > MAX_OBJECT_NAME_LENGTH) {
            return LONG_KEY_PREFIX + sha256Hex(keyBytes);
        }
        return name;
    }

    /**
     * Build the payload for a long key: 4-byte big-endian key length, the key
     * bytes, then the value bytes (empty for a null value).
     */
    private static byte[] payloadWithKey(byte[] keyBytes, byte[] valueBytes) {
        ByteBuffer buf = ByteBuffer.allocate(4 + keyBytes.length + valueBytes.length);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.put(valueBytes);
        return buf.array();
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new DebeziumException("SHA-256 not available", e);
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        return executor.submit(() -> {
            Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
            for (ByteBuffer key : keys) {
                result.put(key, data.get(key));
            }
            return result;
        });
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, OffsetStore.Callback<Void> callback) {
        return executor.submit(() -> {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                LOGGER.debug("Setting offset with key {} and value {}",
                        fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
                data.put(entry.getKey(), entry.getValue());
            }
            // Only the changed keys are written; the engine batches changed
            // offsets into set(), so this keeps writes proportional to the
            // number of changed offsets rather than the whole map.
            save(values.keySet());
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            return null;
        });
    }

    @Override
    public OffsetStorageReader createReader(String namespace) {
        return new DefaultOffsetStorageReader(this, namespace);
    }

    @Override
    public OffsetStorageWriter createWriter(String namespace) {
        return new DefaultOffsetStorageWriter(this, namespace);
    }

    private String fromByteBuffer(ByteBuffer data) {
        return (data != null) ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer())) : null;
    }

    /**
     * Copy the remaining bytes of a buffer into a fresh byte array.
     * <p>
     * Unlike {@link ByteBuffer#array()}, this works for any buffer kind
     * (direct, read-only, sliced, ...) and only copies the bytes between the
     * buffer's position and limit.
     */
    private static byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private void executeWithRetry(NatsOperation operation) throws Exception {
        Exception lastException = null;
        int attempts = 0;
        int maxRetries = config.isRetryEnabled() ? config.getMaxRetries() : 0;

        while (attempts <= maxRetries) {
            try {
                operation.execute();
                return;
            }
            catch (Exception e) {
                lastException = e;
                attempts++;

                if (attempts <= maxRetries) {
                    LOGGER.warn("NATS operation failed (attempt {}/{}), retrying in {}ms",
                            attempts, maxRetries + 1, config.getRetryDelayMs(), e);

                    try {
                        Thread.sleep(config.getRetryDelayMs());
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new DebeziumException("Interrupted during retry delay", ie);
                    }
                }
                else {
                    LOGGER.error("NATS operation failed after {} attempts", attempts, e);
                }
            }
        }

        throw lastException;
    }

    @FunctionalInterface
    private interface NatsOperation {
        void execute() throws Exception;
    }
}
