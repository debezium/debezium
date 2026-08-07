/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.offset;

import java.nio.ByteBuffer;
import java.time.Duration;
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
import io.debezium.storage.redis.RedisClient;
import io.debezium.storage.redis.RedisClientConnectionException;
import io.debezium.storage.redis.RedisConnection;
import io.smallrye.mutiny.Uni;

/**
 * Implementation of OffsetStore that saves to Redis
 * @author Oren Elias
 */

public class RedisOffsetBackingStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOffsetBackingStore.class);

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor;

    private RedisOffsetBackingStoreConfig config;

    private volatile RedisClient client;

    public RedisClient getRedisClient() {
        return client;
    }

    public void setRedisClient(RedisClient client) {
        this.client = client;
    }

    void connect() {
        closeClient();
        RedisConnection redisConnection = RedisConnection.getInstance(config);
        client = redisConnection.getRedisClient(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, config.isWaitEnabled(), config.getWaitTimeout(),
                config.isWaitRetryEnabled(), config.getWaitRetryDelay());
    }

    @Override
    public void configure(Configuration config) {
        this.config = new RedisOffsetBackingStoreConfig(config);
    }

    public void configure(RedisOffsetBackingStoreConfig config) {
        this.config = config;
    }

    @Override
    public synchronized void start() {
        executor = Executors.newFixedThreadPool(1, r -> {
            Thread t = new Thread(r, RedisOffsetBackingStore.class.getSimpleName());
            t.setDaemon(false);
            return t;
        });
        LOGGER.info("Starting RedisOffsetBackingStore");
        this.connect();
        this.load();
    }

    @VisibleForTesting
    synchronized void startNoLoad() {
        executor = Executors.newFixedThreadPool(1, r -> {
            Thread t = new Thread(r, RedisOffsetBackingStore.class.getSimpleName());
            t.setDaemon(false);
            return t;
        });
        this.connect();
    }

    private void closeClient() {
        if (client != null) {
            client.close(); // Disconnect from Redis
            client = null;
        }
    }

    @Override
    public synchronized void stop() {
        closeClient();
        if (executor != null) {
            executor.shutdown();
            try {
                // Timeout taken from Kafka, where it's hard-coded as well. Can be make configurable later on.
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
        // Nothing to do since this doesn't maintain any outstanding connections/data
        LOGGER.info("Stopped RedisOffsetBackingStore");
    }

    /**
    * Load offsets from Redis keys
    */
    @VisibleForTesting
    void load() {
        // fetch the value from Redis
        Map<String, String> offsets = Uni.createFrom().item(() -> {
            if (client == null) {
                throw new RedisClientConnectionException(new RuntimeException("Redis client is null"));
            }
            return (Map<String, String>) client.hgetAll(config.getRedisKeyName());
        })
                // handle failures and retry
                .onFailure().invoke(
                        f -> {
                            LOGGER.warn("Reading from Redis offset store failed with " + f);
                            LOGGER.warn("Will retry");
                        })
                .onFailure(RedisClientConnectionException.class).invoke(
                        f -> {
                            LOGGER.warn("Attempting to reconnect to Redis");
                            this.connect();
                        })
                // retry on failure with backoff
                .onFailure().retry().withBackOff(Duration.ofMillis(config.getInitialRetryDelay()), Duration.ofMillis(config.getMaxRetryDelay())).indefinitely()
                // write success trace message
                .invoke(
                        item -> {
                            LOGGER.trace("Offsets fetched from Redis: " + item);
                        })
                .await().indefinitely();
        this.data = new HashMap<>();
        LOGGER.info("Offsets: {}", offsets);

        for (Map.Entry<String, String> mapEntry : offsets.entrySet()) {
            ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey().getBytes()) : null;
            ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue().getBytes()) : null;
            data.put(key, value);
        }
    }

    /**
    * Save offsets to Redis keys
    */
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
            byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
            // set the value in Redis
            Uni.createFrom().item(() -> {
                if (client == null) {
                    throw new RedisClientConnectionException(new RuntimeException("Redis client is null"));
                }
                return (Long) client.hset(config.getRedisKeyName().getBytes(), key, value);
            })
                    // handle failures and retry
                    .onFailure().invoke(
                            f -> {
                                LOGGER.warn("Writing to Redis offset store failed with " + f);
                                LOGGER.warn("Will retry");
                            })
                    .onFailure(RedisClientConnectionException.class).invoke(
                            f -> {
                                LOGGER.warn("Attempting to reconnect to Redis");
                                this.connect();
                            })
                    // retry on failure with backoff
                    .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(2)).indefinitely()
                    // write success trace message
                    .invoke(
                            item -> {
                                LOGGER.trace("Offsets written to Redis: " + value);
                            })
                    .await().indefinitely();
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(() -> {
            Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
            for (ByteBuffer key : keys) {
                result.put(key, data.get(key));
            }
            return result;
        });
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final OffsetStore.Callback<Void> callback) {
        return executor.submit(() -> {
            data.putAll(values);
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
}
