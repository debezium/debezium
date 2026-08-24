/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;

/**
 * Unit tests for the parts of the Lettuce client that need no running Redis.
 */
class LettuceClientTest {

    private static final String PROP_PREFIX = "offset.storage.redis.";

    @Test
    @DisplayName("the pipelined batch budget scales with the batch size, matching the per-response Jedis timeout")
    public void batchTimeoutShouldScaleWithTheBatchSize() {
        assertEquals(2000L, LettuceClient.batchTimeoutMs(2000L, 1));
        assertEquals(20_000L, LettuceClient.batchTimeoutMs(2000L, 10));
        assertEquals(2_000_000L, LettuceClient.batchTimeoutMs(2000L, 1000));
    }

    @Test
    @DisplayName("an empty batch still gets a usable budget")
    public void batchTimeoutShouldFallBackToASingleCommandTimeout() {
        assertEquals(2000L, LettuceClient.batchTimeoutMs(2000L, 0));
    }

    @Test
    @DisplayName("a batch large enough to overflow the budget falls back instead of going negative")
    public void batchTimeoutShouldNotOverflowIntoANegativeBudget() {
        // A negative budget would make awaitAll give up immediately, turning a configuration mistake into
        // an instant failure of every batch.
        long overflowing = LettuceClient.batchTimeoutMs(Long.MAX_VALUE / 2, 1000);
        assertTrue(overflowing > 0, "expected a positive budget but got " + overflowing);
    }

    @Test
    @DisplayName("cluster mode is rejected before any connection attempt")
    public void shouldRejectClusterModeUpFront() {
        Map<String, String> props = new HashMap<>();
        // Deliberately unreachable: the rejection must happen before the client tries to connect.
        props.put(PROP_PREFIX + "address", "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3");
        props.put(PROP_PREFIX + "cluster.enabled", "true");
        props.put(PROP_PREFIX + "client.library", RedisClientLibrary.LETTUCE.getValue());
        RedisOffsetBackingStoreConfig config = new RedisOffsetBackingStoreConfig(Configuration.from(props));

        DebeziumException thrown = assertThrows(DebeziumException.class,
                () -> RedisConnection.getInstance(config)
                        .getRedisClient(RedisConnection.DEBEZIUM_OFFSETS_CLIENT_NAME, false, 0, false, 0));

        assertTrue(thrown.getMessage().contains("Cluster"),
                "the error should name cluster mode as the unsupported feature, but was: " + thrown.getMessage());
    }

    @Test
    @DisplayName("a missing Lettuce driver is reported with an actionable message, not a NoClassDefFoundError")
    public void shouldRejectAMissingLettuceDriver() {
        // The distribution archive no longer bundles lettuce-core, so this is the message a user sees when
        // they flip redis.client.library=lettuce without adding the driver themselves.
        DebeziumException thrown = assertThrows(DebeziumException.class,
                () -> RedisConnection.requireLettuceDriver(new DriverHidingClassLoader()));

        assertTrue(thrown.getMessage().contains("io.lettuce:lettuce-core"),
                "expected the artifact coordinates in the message but got: " + thrown.getMessage());
        assertTrue(thrown.getMessage().contains("redis.client.library=jedis"),
                "expected the fallback to be suggested but got: " + thrown.getMessage());
    }

    @Test
    @DisplayName("the driver check passes when lettuce-core is on the classpath")
    public void shouldAcceptAPresentLettuceDriver() {
        RedisConnection.requireLettuceDriver(getClass().getClassLoader());
    }

    /**
     * Stands in for a deployment that selected the Lettuce client without putting the driver on the
     * classpath. Everything else resolves normally so that only the driver lookup is exercised.
     */
    private static final class DriverHidingClassLoader extends ClassLoader {

        private DriverHidingClassLoader() {
            super(LettuceClientTest.class.getClassLoader());
        }

        @Override
        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("io.lettuce.")) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }
}
