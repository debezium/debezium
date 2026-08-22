/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;

/**
 * Integration tests for {@link LettuceClient} against a single (standalone) Redis instance.
 */
@Testcontainers
class LettuceClientIT {

    private static final String REDIS_CONTAINER_IMAGE = "redis:5.0.3-alpine";
    private static final String PROP_PREFIX = "offset.storage.redis.";
    private static final String CLIENT_NAME = RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME;

    @Container
    public GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse(REDIS_CONTAINER_IMAGE))
            .withExposedPorts(6379);

    private RedisClient client;

    @BeforeEach
    public void setUp() {
        redis.start();
        Map<String, String> props = new HashMap<>();
        props.put(PROP_PREFIX + "address", redis.getHost() + ":" + redis.getFirstMappedPort());
        props.put(PROP_PREFIX + "client.library", RedisClientLibrary.LETTUCE.getValue());
        RedisOffsetBackingStoreConfig config = new RedisOffsetBackingStoreConfig(Configuration.from(props));
        client = RedisConnection.getInstance(config).getRedisClient(CLIENT_NAME, false, 0, false, 0);
    }

    @AfterEach
    public void tearDown() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (redis != null) {
            redis.stop();
        }
    }

    @Test
    @DisplayName("Lettuce client should be selected when redis.client.library=lettuce")
    public void shouldCreateLettuceClient() {
        assertTrue(client instanceof LettuceClient, "Expected a LettuceClient instance but got " + client);
    }

    @Test
    @DisplayName("xadd/xrange/xlen round-trip on a stream")
    public void shouldWriteAndReadStream() {
        String key = "test:stream";
        Map<String, String> entry = new HashMap<>();
        entry.put("field", "value");

        String id = client.xadd(key, entry);
        assertNotNull(id, "XADD should return a stream entry id");
        assertEquals(1L, client.xlen(key));

        List<Map<String, String>> range = client.xrange(key);
        assertEquals(1, range.size());
        assertEquals("value", range.get(0).get("field"));
    }

    @Test
    @DisplayName("pipelined xadd writes all entries")
    public void shouldWritePipelinedStream() {
        String key = "test:stream:pipeline";
        List<SimpleEntry<String, Map<String, String>>> hashes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            hashes.add(new SimpleEntry<>(key, Map.of("i", String.valueOf(i))));
        }

        List<String> ids = client.xadd(hashes);
        assertEquals(3, ids.size());
        assertEquals(3L, client.xlen(key));
    }

    @Test
    @DisplayName("hset/hgetAll round-trip on a hash")
    public void shouldWriteAndReadHash() {
        String key = "test:hash";
        client.hset(key.getBytes(), "field".getBytes(), "value".getBytes());

        Map<String, String> hash = client.hgetAll(key);
        assertEquals("value", hash.get("field"));
    }

    @Test
    @DisplayName("info and clientList are supported")
    public void shouldReturnServerMetadata() {
        assertNotNull(client.info("server"));
        assertTrue(client.clientList().contains(CLIENT_NAME), "CLIENT LIST should report the configured client name");
    }
}
