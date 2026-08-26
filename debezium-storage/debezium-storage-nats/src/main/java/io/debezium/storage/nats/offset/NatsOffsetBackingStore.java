/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
 *
 * @author Nick Chomey
 */
public class NatsOffsetBackingStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsOffsetBackingStore.class);
    private static final Base64.Encoder OBJECT_NAME_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder OBJECT_NAME_DECODER = Base64.getUrlDecoder();

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
            throw new RuntimeException("Failed to connect to NATS Object Store", e);
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
                    ByteBuffer key = ByteBuffer.wrap(OBJECT_NAME_DECODER.decode(objectName));
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    objectStore.get(objectName, baos);
                    // A zero-byte object is a tombstone for a null value
                    ByteBuffer value = baos.size() == 0 ? null : ByteBuffer.wrap(baos.toByteArray());
                    data.put(key, value);
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
            throw new RuntimeException("Failed to load offsets", e);
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
                    String objectName = OBJECT_NAME_ENCODER.encodeToString(toByteArray(key));
                    byte[] valueBytes = data.get(key) != null ? toByteArray(data.get(key)) : new byte[0];
                    try {
                        objectStore.put(objectName, valueBytes);
                        LOGGER.trace("Stored offset object '{}' ({} bytes) in NATS Object Store",
                                objectName, valueBytes.length);
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
            throw new RuntimeException("Failed to save offsets", e);
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
                        throw new RuntimeException("Interrupted during retry delay", ie);
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
