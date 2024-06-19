package io.debezium.connector.mongodb;

import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of OffsetBackingStore that saves to Etcd
 *
 * TODO: currently it's mostly copy-paste from the RedisOffsetBackingStore, need to fill out most of the blanks
 */
public class EtcdOffsetBackingStore extends MemoryOffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdOffsetBackingStore.class);

    private static final String CONFIGURATION_FIELD_PREFIX_STRING = "offset.storage.etcd.";


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

    @Override
    public synchronized void start() {
        super.start();
        LOGGER.info("Starting EtcdOffsetBackingStore");
        this.load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        LOGGER.info("Stopped EtcdOffsetBackingStore");
    }

    /**
     * Load offsets from Etcd keys
     */
    private void load() {
        // fetch the value from Redis
        Map<String, String> offsets = loadOffsets();
        this.data = new HashMap<>();
        for (Map.Entry<String, String> mapEntry : offsets.entrySet()) {
            ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey().getBytes()) : null;
            ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue().getBytes()) : null;
            data.put(key, value);
        }
    }


    /**
     * Save offsets to Etcd keys
     */
    @Override
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
            byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
            // set the value in Redis
            saveOffset(key, value);
        }
    }


    private Map<String, String> loadOffsets() { return Collections.emptyMap();
    }

    private void saveOffset(byte[] key, byte[] value) {
    }
}
