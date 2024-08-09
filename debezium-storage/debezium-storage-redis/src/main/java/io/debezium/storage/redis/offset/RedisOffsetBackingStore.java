/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.offset;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.storage.redis.RedisClient;
import io.debezium.storage.redis.RedisConnection;
import io.smallrye.mutiny.Uni;

/**
 * Implementation of OffsetBackingStore that saves to Redis
 * @author Oren Elias
 */

public class RedisOffsetBackingStore extends MemoryOffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOffsetBackingStore.class);

    private RedisOffsetBackingStoreConfig config;

    private RedisClient client;

    public RedisClient getRedisClient() {
        return client;
    }

    public void setRedisClient(RedisClient client) {
        this.client = client;
    }

    void connect() {
        closeClient();
        RedisConnection redisConnection = new RedisConnection(config.getAddress(), config.getDbIndex(), config.getUser(), config.getPassword(),
                config.getConnectionTimeout(), config.getSocketTimeout(), config.isSslEnabled());
        client = redisConnection.getRedisClient(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, config.isWaitEnabled(), config.getWaitTimeout(),
                config.isWaitRetryEnabled(), config.getWaitRetryDelay());
    }

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        Configuration configuration = Configuration.from(config.originalsStrings());
        this.config = new RedisOffsetBackingStoreConfig(configuration);
    }

    public void configure(RedisOffsetBackingStoreConfig config) {
        this.config = config;
    }

    @Override
    public synchronized void start() {
        super.start();
        LOGGER.info("Starting RedisOffsetBackingStore");
        this.connect();
        this.load();
    }

    @VisibleForTesting
    synchronized void startNoLoad() {
        super.start();
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
        super.stop();
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
            return (Map<String, String>) client.hgetAll(config.getRedisKeyName());
        })
                // handle failures and retry
                .onFailure().invoke(
                        f -> {
                            LOGGER.warn("Reading from Redis offset store failed with " + f);
                            LOGGER.warn("Will retry");
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
        LOGGER.info("Offsets: {}", offsets.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", ", "{ ", " }")));

        for (Map.Entry<String, String> mapEntry : offsets.entrySet()) {
            ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey().getBytes()) : null;
            ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue().getBytes()) : null;
            data.put(key, value);
        }
    }

    /**
    * Save offsets to Redis keys
    */
    @Override
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
            byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
            // set the value in Redis
            Uni.createFrom().item(() -> {
                return (Long) client.hset(config.getRedisKeyName().getBytes(), key, value);
            })
                    // handle failures and retry
                    .onFailure().invoke(
                            f -> {
                                LOGGER.warn("Writing to Redis offset store failed with " + f);
                                LOGGER.warn("Will retry");
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
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return null;
    }
}
