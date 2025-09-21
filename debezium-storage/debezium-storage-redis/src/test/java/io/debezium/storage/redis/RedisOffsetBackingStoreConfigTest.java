/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;

/**
 * Unit tests for verifying Redis cluster enablement flag handling via configuration.
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
}
