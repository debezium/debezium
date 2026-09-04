/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;

/**
 * Unit tests for verifying Redis Cluster enablement and client library selection via configuration.
 */
public class RedisOffsetBackingStoreConfigTest {

    private static final String PREFIX = "offset.storage.";

    @Test
    public void clusterEnabledDefaultsToFalse() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "redis.address", "localhost:6379");
        RedisOffsetBackingStoreConfig cfg = new RedisOffsetBackingStoreConfig(Configuration.from(props));
        assertFalse(cfg.isClusterEnabled(), "Cluster mode should be disabled by default");
    }

    @Test
    public void clusterEnabledCanBeEnabled() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "redis.address", "localhost:6379");
        props.put(PREFIX + "redis.cluster.enabled", "true");
        RedisOffsetBackingStoreConfig cfg = new RedisOffsetBackingStoreConfig(Configuration.from(props));
        assertTrue(cfg.isClusterEnabled(), "Cluster mode should be enabled when property is set to true");
    }

    @Test
    public void clientLibraryDefaultsToJedis() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "redis.address", "localhost:6379");
        RedisOffsetBackingStoreConfig cfg = new RedisOffsetBackingStoreConfig(Configuration.from(props));
        assertEquals(RedisClientLibrary.JEDIS, cfg.getClientLibrary(), "Client library should default to jedis");
    }

    @Test
    public void clientLibraryCanBeSetToLettuce() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "redis.address", "localhost:6379");
        props.put(PREFIX + "redis.client.library", RedisClientLibrary.LETTUCE.getValue());
        RedisOffsetBackingStoreConfig cfg = new RedisOffsetBackingStoreConfig(Configuration.from(props));
        assertEquals(RedisClientLibrary.LETTUCE, cfg.getClientLibrary(), "Client library should be lettuce");
    }

    @Test
    public void invalidClientLibraryIsRejected() {
        Map<String, String> props = new HashMap<>();
        props.put(PREFIX + "redis.address", "localhost:6379");
        props.put(PREFIX + "redis.client.library", "invalid");
        assertThrows(DebeziumException.class,
                () -> new RedisOffsetBackingStoreConfig(Configuration.from(props)),
                "An unsupported client library value should be rejected during validation");
    }
}
