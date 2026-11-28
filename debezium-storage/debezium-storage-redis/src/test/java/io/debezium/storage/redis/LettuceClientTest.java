/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.storage.redis.offset.RedisOffsetBackingStoreConfig;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandType;

/**
 * Unit tests for the parts of the Lettuce client that need no running Redis.
 */
class LettuceClientTest {

    private static final String PROP_PREFIX = "offset.storage.redis.";

    @Test
    @DisplayName("a stalled pipeline fails within one response timeout, not one timeout per command")
    public void stalledPipelineShouldFailWithinASingleResponseTimeout() {
        // Only the first of 500 responses ever arrives. A budget scaled by the batch size would keep the
        // caller waiting for 500 x 200 ms = 100 s before reconnecting.
        final List<AsyncCommand<String, String, String>> futures = pendingXadds(500);
        futures.get(0).complete("1-0");

        final long started = System.nanoTime();
        final RedisCommandTimeoutException thrown = assertThrows(RedisCommandTimeoutException.class,
                () -> LettuceClient.awaitInOrder(futures, 200));
        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertTrue(elapsedMs < 5_000, "expected the stall to be detected after about 200 ms but it took " + elapsedMs + " ms");
        assertTrue(thrown.getMessage().contains("command 2 of 500"), "the error should name the stalled command, but was: " + thrown.getMessage());
    }

    @Test
    @DisplayName("a pipeline that keeps making progress is not cut short by the response timeout")
    public void progressingPipelineShouldOutlastASingleResponseTimeout() throws Exception {
        // Every response arrives 150 ms after the previous one, so the whole batch takes about three times
        // longer than the 300 ms response timeout without ever stalling for that long.
        final List<AsyncCommand<String, String, String>> futures = pendingXadds(6);
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            for (int i = 0; i < futures.size(); i++) {
                final AsyncCommand<String, String, String> future = futures.get(i);
                final String id = (i + 1) + "-0";
                executor.schedule(() -> future.complete(id), 150L * (i + 1), TimeUnit.MILLISECONDS);
            }

            assertEquals(List.of("1-0", "2-0", "3-0", "4-0", "5-0", "6-0"), LettuceClient.awaitInOrder(futures, 300));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("a failed pipelined command surfaces unwrapped, so server errors stay distinct from connection failures")
    public void failedPipelinedCommandShouldSurfaceUnwrapped() {
        final List<AsyncCommand<String, String, String>> futures = pendingXadds(2);
        futures.get(0).complete("1-0");
        futures.get(1).completeExceptionally(new RedisCommandExecutionException("LOADING Redis is loading the dataset in memory"));

        final RedisCommandExecutionException thrown = assertThrows(RedisCommandExecutionException.class,
                () -> LettuceClient.awaitInOrder(futures, 1_000));

        assertTrue(thrown.getMessage().startsWith("LOADING"), "expected the server error itself but got: " + thrown.getMessage());
    }

    /**
     * Real, unsent Lettuce commands: they stay pending until a test completes them, just as they would while
     * waiting for Redis to answer.
     */
    private static List<AsyncCommand<String, String, String>> pendingXadds(int count) {
        final List<AsyncCommand<String, String, String>> futures = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            futures.add(new AsyncCommand<>(new Command<>(CommandType.XADD, new StatusOutput<>(StringCodec.UTF8))));
        }
        return futures;
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
