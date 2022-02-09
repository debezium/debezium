/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.server.TestConfigSource;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;

/**
 * Integration test that verifies reading and writing offsets from Redis key value store
 *
 * @author Oren Elias
 */
@QuarkusIntegrationTest
@TestProfile(RedisOffsetTestProfile.class)
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)

public class RedisOffsetIT {

    private static final int MESSAGE_COUNT = 4;
    private static final String STREAM_NAME = "testc.inventory.customers";

    protected static Jedis jedis;

    @Test
    @FixFor("DBZ-4509")
    public void testRedisStream() throws Exception {
        jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        Testing.Print.enable();
        final List<StreamEntry> entries = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            final List<StreamEntry> response = jedis.xrange(STREAM_NAME, null, null, MESSAGE_COUNT);
            entries.addAll(response);
            return entries.size() >= MESSAGE_COUNT;
        });

        Map<String, String> redisOffsets = jedis.hgetAll("offsets");
        Assertions.assertThat(redisOffsets.size() > 0).isTrue();
    }

}
