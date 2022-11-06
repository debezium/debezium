/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Integration tests that verify basic reading from PostgreSQL database and writing to Redis stream
 *
 * @author ggaborg
 */
@QuarkusIntegrationTest
@TestProfile(RedisStreamMessageTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamMessageIT {

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis in extended message format
    */
    @Test
    public void testRedisStreamExtendedMessage() throws Exception {
        Testing.Print.enable();

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);

        Long streamLength = jedis.xlen(STREAM_NAME);
        assertTrue("Expected stream length of " + MESSAGE_COUNT, streamLength == MESSAGE_COUNT);

        final List<StreamEntry> entries = jedis.xrange(STREAM_NAME, (StreamEntryID) null, (StreamEntryID) null);
        for (StreamEntry entry : entries) {
            Map<String, String> map = entry.getFields();
            assertTrue("Expected map of size 2", map.size() == 2);
            assertTrue("Expected key's value starting with {\"schema\":...", map.get("key") != null && map.get("key").startsWith("{\"schema\":"));
            assertTrue("Expected values's value starting with {\"schema\":...", map.get("value") != null && map.get("value").startsWith("{\"schema\":"));
        }

        jedis.close();
    }

}
