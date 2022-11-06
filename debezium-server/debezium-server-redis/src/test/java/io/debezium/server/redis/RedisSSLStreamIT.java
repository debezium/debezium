/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * Integration tests for secured Redis
 *
 * @author Oren Elias
 */
@QuarkusIntegrationTest
@TestProfile(RedisSSLStreamTestProfile.class)
public class RedisSSLStreamIT {

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis
    */
    @Test
    public void testRedisStream() throws Exception {
        HostAndPort address = HostAndPort.from(RedisSSLTestResourceLifecycleManager.getRedisContainerAddress());
        Jedis jedis = new Jedis(address.getHost(), address.getPort(), true);
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";
        final String HASH_NAME = "metadata:debezium:offsets";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);

        Long streamLength = jedis.xlen(STREAM_NAME);
        assertTrue("Redis Basic Stream Test Failed", streamLength == MESSAGE_COUNT);

        // wait until the offsets are re-written
        TestUtils.awaitHashSizeGte(jedis, HASH_NAME, 1);

        Map<String, String> redisOffsets = jedis.hgetAll(HASH_NAME);
        assertTrue(redisOffsets.size() > 0);

        jedis.close();
    }
}
