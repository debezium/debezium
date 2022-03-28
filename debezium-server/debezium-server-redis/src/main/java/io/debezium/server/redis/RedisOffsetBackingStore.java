/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.smallrye.mutiny.Uni;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/** 
 * Implementation of OffsetBackingStore that saves to Redis
 * @author Oren Elias
 */

public class RedisOffsetBackingStore extends MemoryOffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOffsetBackingStore.class);

    private static final String CONFIGURATION_FIELD_PREFIX_STRING = "offset.storage.redis.";
    public static final Field PROP_ADDRESS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "address")
            .withDescription("The redis url that will be used to access the database history");

    public static final Field PROP_SSL_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "ssl.enabled")
            .withDescription("Use SSL for Redis connection")
            .withDefault("false");

    public static final Field PROP_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "user")
            .withDescription("The redis url that will be used to access the database history");

    public static final Field PROP_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "password")
            .withDescription("The redis url that will be used to access the database history");

    public static final String DEFAULT_REDIS_KEY_NAME = "metadata:debezium:offsets";
    public static final Field PROP_KEY_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "key")
            .withDescription("The redis key that will be used to store the database history")
            .withDefault(DEFAULT_REDIS_KEY_NAME);

    public static final Integer DEFAULT_RETRY_INITIAL_DELAY = 300;
    public static final Field PROP_RETRY_INITIAL_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.initial.delay.ms")
            .withDescription("Initial retry delay (in ms)")
            .withDefault(DEFAULT_RETRY_INITIAL_DELAY);

    public static final Integer DEFAULT_RETRY_MAX_DELAY = 10000;
    public static final Field PROP_RETRY_MAX_DELAY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.max.delay.ms")
            .withDescription("Maximum retry delay (in ms)")
            .withDefault(DEFAULT_RETRY_MAX_DELAY);

    private static final String SINK_PROP_PREFIX = "debezium.sink.redis.";

    private String redisKeyName;
    private String address;
    private String user;
    private String password;
    private boolean sslEnabled;

    private Jedis client = null;
    private Map<String, String> config;

    private Integer initialRetryDelay;
    private Integer maxRetryDelay;

    public RedisOffsetBackingStore() {

    }

    void connect() {
        HostAndPort address = HostAndPort.from(this.address);

        client = new Jedis(address.getHost(), address.getPort(), this.sslEnabled);

        if (this.user != null) {
            client.auth(this.user, this.password);
        }
        else if (this.password != null) {
            client.auth(this.password);
        }
        else {
            // make sure that client is connected
            client.ping();
        }
    }

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        this.config = config.originalsStrings();
        // fetch the properties. as a fallback, if we did not specify the redis address, we will take it and all the credentials from the sink
        this.address = this.config.get(PROP_ADDRESS.name());
        if (this.address == null) {
            // try to take the connection details from the redis sink
            this.address = this.config.get(SINK_PROP_PREFIX + "address");
            this.user = this.config.get(SINK_PROP_PREFIX + "user");
            this.password = this.config.get(SINK_PROP_PREFIX + "password");
            this.sslEnabled = Boolean.parseBoolean(this.config.get(SINK_PROP_PREFIX + "ssl.enabled"));
        }
        else {
            this.user = this.config.get(PROP_USER.name());
            this.password = this.config.get(PROP_PASSWORD.name());
            this.sslEnabled = Boolean.parseBoolean(this.config.get(PROP_SSL_ENABLED.name()));
        }

        this.redisKeyName = Optional.ofNullable(
                this.config.get(PROP_KEY_NAME.name())).orElse(DEFAULT_REDIS_KEY_NAME);
        // load retry settings
        this.initialRetryDelay = Optional.ofNullable(
                Integer.getInteger(this.config.get(PROP_RETRY_INITIAL_DELAY.name()))).orElse(DEFAULT_RETRY_INITIAL_DELAY);
        this.maxRetryDelay = Optional.ofNullable(
                Integer.getInteger(this.config.get(PROP_RETRY_INITIAL_DELAY.name()))).orElse(DEFAULT_RETRY_MAX_DELAY);

    }

    @Override
    public synchronized void start() {
        super.start();
        LOGGER.info("Starting RedisOffsetBackingStore");
        this.connect();
        this.load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        LOGGER.info("Stopped RedisOffsetBackingStore");
    }

    /**
    * Load offsets from redis keys
    */
    private void load() {
        // fetch the value from Redis
        Map<String, String> offsets = Uni.createFrom().item(() -> {
            return (Map<String, String>) client.hgetAll(this.redisKeyName);
        })
                // handle failures and retry
                .onFailure().invoke(
                        f -> {
                            LOGGER.warn("Reading from offset store failed with " + f);
                            LOGGER.warn("Will retry");
                        })
                .onFailure(JedisConnectionException.class).invoke(
                        f -> {
                            LOGGER.warn("Attempting to reconnect to redis ");
                            this.connect();
                        })
                // retry on failure with backoff
                .onFailure().retry().withBackOff(Duration.ofMillis(initialRetryDelay), Duration.ofMillis(maxRetryDelay)).indefinitely()
                // write success trace message
                .invoke(
                        item -> {
                            LOGGER.trace("Offsets fetched from redis: " + item);
                        })
                .await().indefinitely();
        this.data = new HashMap<>();
        for (Map.Entry<String, String> mapEntry : offsets.entrySet()) {
            ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey().getBytes()) : null;
            ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue().getBytes()) : null;
            data.put(key, value);
        }
    }

    /**
    * Save offsets to redis keys
    */
    @Override
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
            byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
            // set the value in Redis
            Uni.createFrom().item(() -> {
                return (Long) client.hset(this.redisKeyName.getBytes(), key, value);
            })
                    // handle failures and retry
                    .onFailure().invoke(
                            f -> {
                                LOGGER.warn("Writing to offset store failed with " + f);
                                LOGGER.warn("Will retry");
                            })
                    .onFailure(JedisConnectionException.class).invoke(
                            f -> {
                                LOGGER.warn("Attempting to reconnect to redis ");
                                this.connect();
                            })
                    // retry on failure with backoff
                    .onFailure().retry().withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(2)).indefinitely()
                    // write success trace message
                    .invoke(
                            item -> {
                                LOGGER.trace("Record written to offset store in redis: " + value);
                            })
                    .await().indefinitely();
        }
    }
}
