/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.storage.nats.NatsCommonConfig;

/**
 * Tests for NATS-based offset backing store.
 *
 * @author Nick Chomey
 */
@Testcontainers
class NatsOffsetBackingStoreTest {

    private static final String NATS_CONTAINER_IMAGE = "nats:2.12.0-alpine";
    private static final int NATS_PORT = 4222;

    @Container
    @SuppressWarnings("resource")
    public GenericContainer<?> natsContainer = new GenericContainer<>(DockerImageName.parse(NATS_CONTAINER_IMAGE))
            .withExposedPorts(NATS_PORT)
            .withCommand("-js")
            .withLogConsumer(frame -> {
                if (frame != null && frame.getUtf8String() != null) {
                    System.out.print(frame.getUtf8String());
                }
            });

    private String natsUrl;
    private NatsOffsetBackingStore offsetStore;

    @BeforeEach
    public void setUp() {
        natsContainer.start();
        natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getFirstMappedPort();

        offsetStore = new NatsOffsetBackingStore();
        Map<String, String> config = createConfig();
        StandaloneConfig workerConfig = new StandaloneConfig(config);
        offsetStore.configure(workerConfig);
        offsetStore.start();
    }

    @AfterEach
    public void tearDown() {
        if (offsetStore != null) {
            offsetStore.stop();
        }
        if (natsContainer != null) {
            natsContainer.stop();
        }
    }

    private Map<String, String> createConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("offset.storage." + NatsCommonConfig.NATS_URL.name(), natsUrl);
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_BUCKET_NAME.name(), "test-offsets");
        // single object name used; no per-key prefix
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_RETRY_ENABLED.name(), "true");
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_MAX_RETRIES.name(), "3");
        config.put("offset.storage." + NatsOffsetBackingStoreConfig.PROP_RETRY_DELAY_MS.name(), "100");
        // Required Kafka Connect configurations
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("offset.storage", "io.debezium.storage.nats.offset.NatsOffsetBackingStore");
        config.put("offset.storage.file.filename", "/tmp/test-offsets.dat"); // Required for StandaloneConfig
        return config;
    }

    @Test
    public void shouldStartAndStop() {
        // Should start and stop without errors
        assertNotNull(offsetStore);
    }

    @Test
    @Timeout(10)
    public void shouldStoreAndRetrieveOffsets() throws Exception {
        // Prepare test data
        ByteBuffer key1 = ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8));
        ByteBuffer value1 = ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8));
        ByteBuffer key2 = ByteBuffer.wrap("key2".getBytes(StandardCharsets.UTF_8));
        ByteBuffer value2 = ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8));

        Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
        offsets.put(key1, value1);
        offsets.put(key2, value2);

        // Store offsets
        Future<Void> setFuture = offsetStore.set(offsets, null);
        setFuture.get(5, TimeUnit.SECONDS);

        // Retrieve offsets
        Collection<ByteBuffer> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(keys);
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(5, TimeUnit.SECONDS);

        // Verify
        assertThat(retrievedOffsets).hasSize(2);
        assertEquals(value1, retrievedOffsets.get(key1));
        assertEquals(value2, retrievedOffsets.get(key2));
    }

    @Test
    @Timeout(10)
    public void shouldHandleNullValues() throws Exception {
        ByteBuffer key = ByteBuffer.wrap("nullKey".getBytes(StandardCharsets.UTF_8));

        Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
        offsets.put(key, null);

        // Store null value
        Future<Void> setFuture = offsetStore.set(offsets, null);
        setFuture.get(5, TimeUnit.SECONDS);

        // Retrieve
        Collection<ByteBuffer> keys = new ArrayList<>();
        keys.add(key);
        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(keys);
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(5, TimeUnit.SECONDS);

        // Should handle null values gracefully
        assertThat(retrievedOffsets).containsKey(key);
    }

    @Test
    @Timeout(10)
    public void shouldPersistOffsetsAcrossRestarts() throws Exception {
        // Store initial offsets
        ByteBuffer key = ByteBuffer.wrap("persistKey".getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap("persistValue".getBytes(StandardCharsets.UTF_8));

        Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
        offsets.put(key, value);

        Future<Void> setFuture = offsetStore.set(offsets, null);
        setFuture.get(5, TimeUnit.SECONDS);

        // Stop and restart
        offsetStore.stop();

        offsetStore = new NatsOffsetBackingStore();
        Map<String, String> config = createConfig();
        StandaloneConfig workerConfig = new StandaloneConfig(config);
        offsetStore.configure(workerConfig);
        offsetStore.start();

        // Retrieve offsets after restart
        Collection<ByteBuffer> keys = new ArrayList<>();
        keys.add(key);
        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(keys);
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(5, TimeUnit.SECONDS);

        // Should persist across restarts
        assertThat(retrievedOffsets).hasSize(1);
        assertEquals(value, retrievedOffsets.get(key));
    }

    @Test
    @Timeout(10)
    public void shouldHandleCallbackOnSet() throws Exception {
        ByteBuffer key = ByteBuffer.wrap("callbackKey".getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap("callbackValue".getBytes(StandardCharsets.UTF_8));

        Map<ByteBuffer, ByteBuffer> offsets = new HashMap<>();
        offsets.put(key, value);

        // Use callback to track completion
        final boolean[] callbackInvoked = { false };
        Callback<Void> callback = new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                callbackInvoked[0] = true;
                assertNull(error);
            }
        };

        Future<Void> setFuture = offsetStore.set(offsets, callback);
        setFuture.get(5, TimeUnit.SECONDS);

        // Callback should be invoked
        assertTrue(callbackInvoked[0]);
    }

    @Test
    @Timeout(10)
    public void shouldHandleEmptyOffsets() throws Exception {
        Map<ByteBuffer, ByteBuffer> emptyOffsets = new HashMap<>();

        Future<Void> setFuture = offsetStore.set(emptyOffsets, null);
        setFuture.get(5, TimeUnit.SECONDS);

        // Should handle empty offsets without error
        Collection<ByteBuffer> keys = new ArrayList<>();
        Future<Map<ByteBuffer, ByteBuffer>> getFuture = offsetStore.get(keys);
        Map<ByteBuffer, ByteBuffer> retrievedOffsets = getFuture.get(5, TimeUnit.SECONDS);

        assertThat(retrievedOffsets).isEmpty();
    }

    @Test
    public void shouldReturnNullForConnectorPartitions() {
        // This method is not implemented and should return null
        assertNull(offsetStore.connectorPartitions("test-connector"));
    }
}
