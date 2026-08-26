/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
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
 * Uses Java serialization to store all offsets in a single Object Store entry.
 *
 * @author Nick Chomey
 */
public class NatsOffsetBackingStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsOffsetBackingStore.class);
    private static final String OFFSET_OBJECT_NAME = "debezium-offsets";

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

            try {
                // Try to get the serialized offset data from Object Store
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectInfo info = objectStore.get(OFFSET_OBJECT_NAME, baos);

                if (info != null && baos.size() > 0) {
                    // Deserialize the offset map using Java serialization
                    byte[] offsetData = baos.toByteArray();
                    deserializeOffsets(offsetData);
                    LOGGER.debug("Loaded {} offsets from NATS Object Store", data.size());
                }
                else {
                    LOGGER.debug("No existing offsets found in NATS Object Store");
                }
            }
            catch (Exception e) {
                // Object doesn't exist yet - this is normal for first run
                LOGGER.debug("No existing offsets found in NATS Object Store (object not found)");
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
        try {
            // Serialize the entire offset map using Java serialization
            byte[] offsetData = serializeOffsets();

            executeWithRetry(() -> {
                try {
                    ByteArrayInputStream bais = new ByteArrayInputStream(offsetData);
                    LOGGER.debug("Putting offsets object to NATS Object Store bucket='{}'", config.getBucketName());
                    objectStore.put(OFFSET_OBJECT_NAME, bais);
                    LOGGER.trace("Stored {} bytes of offset data to NATS Object Store", offsetData.length);
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
            });

            LOGGER.debug("Successfully saved {} offsets to NATS Object Store", data.size());
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
            save();
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
     * Serialize all offsets using Java serialization for storage in NATS Object
     * Store.
     * This is much simpler than JSON and handles ByteBuffers natively.
     */
    private byte[] serializeOffsets() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        // Create a serializable map from the data
        Map<byte[], byte[]> serializableMap = new HashMap<>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : data.entrySet()) {
            if (entry.getKey() != null) {
                byte[] key = toByteArray(entry.getKey());
                byte[] value = entry.getValue() != null ? toByteArray(entry.getValue()) : null;
                serializableMap.put(key, value);
            }
        }

        oos.writeObject(serializableMap);
        oos.close();

        return baos.toByteArray();
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

    /**
     * Deserialize offsets from Java serialization and populate the data map.
     */
    @SuppressWarnings("unchecked")
    private void deserializeOffsets(byte[] offsetData) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(offsetData);
        ObjectInputStream ois = new ObjectInputStream(bais);

        Map<byte[], byte[]> serializableMap = (Map<byte[], byte[]>) ois.readObject();
        ois.close();

        // Convert back to ByteBuffer map
        data.clear();
        for (Map.Entry<byte[], byte[]> entry : serializableMap.entrySet()) {
            ByteBuffer key = ByteBuffer.wrap(entry.getKey());
            ByteBuffer value = entry.getValue() != null ? ByteBuffer.wrap(entry.getValue()) : null;
            data.put(key, value);
        }
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
