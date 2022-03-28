/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.server.TestConfigSource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * Integration tests that verify basic reading from PostgreSQL database and writing to Redis stream
 * and retry mechanism in case of connectivity issues or OOM in Redis
 *
 * @author M Sazzadul Hoque
 */
@QuarkusIntegrationTest
@TestProfile(RedisSSLStreamTestProfile.class)
public class RedisSSLStreamIT {

    private Long getStreamLength(Jedis jedis, String streamName, int expectedLength) {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            return jedis.xlen(streamName) == expectedLength;
        });

        return jedis.xlen(streamName);
    }

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis
    */
    @Test
    public void testRedisStream() throws Exception {
        HostAndPort address = HostAndPort.from(RedisSSLTestResourceLifecycleManager.getRedisContainerAddress());
        Jedis jedis = new Jedis(address.getHost(), address.getPort(), true);
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        Long streamLength = getStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);
        assertTrue("Redis Basic Stream Test Failed", streamLength == MESSAGE_COUNT);

        // wait until the offsets are re-written
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map<String, String> redisOffsets = jedis.hgetAll("metadata:debezium:offsets");
            return redisOffsets.size() > 0;
        });
        Map<String, String> redisOffsets = jedis.hgetAll("metadata:debezium:offsets");
        assertTrue(redisOffsets.size() > 0);

        jedis.close();

    }

}
