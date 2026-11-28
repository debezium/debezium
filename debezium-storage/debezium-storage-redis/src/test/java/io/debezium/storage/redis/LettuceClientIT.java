/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;

import redis.clients.jedis.Jedis;

/**
 * Integration tests for {@link LettuceClient} against a single (standalone) Redis instance.
 */
@Testcontainers
class LettuceClientIT {

    private static final String REDIS_CONTAINER_IMAGE = "redis:5.0.3-alpine";
    private static final String PROP_PREFIX = "offset.storage.redis.";
    private static final String CLIENT_NAME = RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME;
    private static final String OOM_MESSAGE = "OOM command not allowed when used memory > 'maxmemory'";

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
    @DisplayName("hset reports 1 for a new field and 0 for an update, matching the Jedis contract")
    public void shouldReportWhetherTheFieldWasCreated() {
        // RedisOffsetBackingStore.save() returns this value straight to Kafka Connect, and Lettuce natively
        // answers with a Boolean, so the mapping to the Jedis-shaped count is worth pinning down.
        String key = "test:hash:created";
        assertEquals(1L, client.hset(key.getBytes(), "field".getBytes(), "value".getBytes()));
        assertEquals(0L, client.hset(key.getBytes(), "field".getBytes(), "other".getBytes()));

        assertEquals("other", client.hgetAll(key).get("field"));
    }

    @Test
    @DisplayName("hset round-trips non-ASCII payloads through the string codec")
    public void shouldRoundTripNonAsciiHashValues() {
        String key = "test:hash:utf8";
        String value = "\uc624\ud504\uc14b-\u30c6\u30b9\u30c8-\u00e9\u00e0";
        client.hset(key.getBytes(StandardCharsets.UTF_8), "field".getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));

        assertEquals(value, client.hgetAll(key).get("field"));
    }

    @Test
    @DisplayName("hset stores its arguments byte for byte, even when they are not valid UTF-8")
    public void shouldStoreHashArgumentsUnchanged() {
        // Decoding these as UTF-8 would turn both fields into U+FFFD, so they would collapse into one field
        // and the original bytes could not be recovered, not even by switching back to Jedis.
        final byte[] key = "test:hash:binary".getBytes(StandardCharsets.UTF_8);
        final byte[] firstField = { (byte) 0xC3, (byte) 0x28 };
        final byte[] secondField = { (byte) 0xFF };
        final byte[] value = { (byte) 0x80, 0x00, (byte) 0xFE };

        assertEquals(1L, client.hset(key, firstField, value));
        assertEquals(1L, client.hset(key, secondField, value));

        // Read back over the binary-safe Jedis API, since hgetAll decodes to String for every client.
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getFirstMappedPort())) {
            assertEquals(2L, jedis.hlen(key));
            assertArrayEquals(value, jedis.hget(key, firstField));
            assertArrayEquals(value, jedis.hget(key, secondField));
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(RedisClientLibrary.class)
    @DisplayName("writes refused at maxmemory keep their place in a pipelined batch instead of failing it")
    public void outOfMemoryResponsesShouldKeepTheirPlaceInABatch(RedisClientLibrary library) throws Exception {
        // The Debezium Server Redis sink commits the entries whose response is an ID and retries only those
        // answered with the OOM error, so every response must stay at the position of its entry. Run against
        // both drivers, since the sink relies on the Jedis shape of this list.
        final String key = "test:stream:oom";
        final Map<String, String> entry = Map.of("payload", "x".repeat(200 * 1024));
        final List<SimpleEntry<String, Map<String, String>>> batch = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            batch.add(new SimpleEntry<>(key, entry));
        }

        // Room for a few entries only: the first writes of the batch succeed, the later ones are refused.
        final long usedMemory = Long.parseLong(client.info("memory").lines()
                .filter(line -> line.startsWith("used_memory:"))
                .findFirst().orElseThrow().substring("used_memory:".length()).trim());
        redis.execInContainer("redis-cli", "config", "set", "maxmemory-policy", "noeviction");
        redis.execInContainer("redis-cli", "config", "set", "maxmemory", String.valueOf(usedMemory + 1024 * 1024));

        final RedisClient batchClient = library == RedisClientLibrary.LETTUCE ? client : newClient(library);
        try {
            final List<String> responses = batchClient.xadd(batch);

            assertEquals(batch.size(), responses.size(), "expected one response per entry but got " + responses);
            final long written = responses.stream().filter(response -> response.matches("\\d+-\\d+")).count();
            final long refused = responses.stream().filter(response -> response.contains(OOM_MESSAGE)).count();
            assertEquals(batch.size(), written + refused, "every response should be an ID or the OOM error: " + responses);
            assertTrue(written > 0 && refused > 0, "expected a batch mixing written and refused entries: " + responses);
            assertTrue(responses.get(0).matches("\\d+-\\d+"), "the first entry fits and should be written: " + responses.get(0));
            assertTrue(responses.get(batch.size() - 1).contains(OOM_MESSAGE), "the last entry should be refused");
            assertEquals(written, client.xlen(key), "the IDs should account for exactly the entries in the stream");
        }
        finally {
            if (batchClient != client) {
                batchClient.close();
            }
        }
    }

    private RedisClient newClient(RedisClientLibrary library) {
        Map<String, String> props = new HashMap<>();
        props.put(PROP_PREFIX + "address", redis.getHost() + ":" + redis.getFirstMappedPort());
        props.put(PROP_PREFIX + "client.library", library.getValue());
        RedisOffsetBackingStoreConfig config = new RedisOffsetBackingStoreConfig(Configuration.from(props));
        return RedisConnection.getInstance(config).getRedisClient(CLIENT_NAME, false, 0, false, 0);
    }

    @Test
    @DisplayName("info and clientList are supported")
    public void shouldReturnServerMetadata() {
        assertNotNull(client.info("server"));
        assertTrue(client.clientList().contains(CLIENT_NAME), "CLIENT LIST should report the configured client name");
    }
}
