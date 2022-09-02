/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.server.TestConfigSource;
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
 * in compact message format
 *
 * @author ggaborg
 */
@QuarkusIntegrationTest
@TestProfile(RedisStreamCompactMessageTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamCompactMessageIT {

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis in compact message format
    */
    @Test
    public void testRedisStreamCompactMessage() throws Exception {
        Testing.Print.enable();

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        final List<StreamEntry> entries = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            final List<StreamEntry> response = jedis.xrange(STREAM_NAME, (StreamEntryID) null, (StreamEntryID) null, MESSAGE_COUNT);
            entries.addAll(response);
            return entries.size() == MESSAGE_COUNT;
        });

        Long streamLength = jedis.xlen(STREAM_NAME);
        assertTrue("Expected stream length of " + MESSAGE_COUNT, streamLength == MESSAGE_COUNT);
        for (StreamEntry entry : entries) {
            Map<String, String> map = entry.getFields();
            assertTrue("Expected map of size 1", map.size() == 1);
            Map.Entry<String, String> mapEntry = map.entrySet().iterator().next();
            assertTrue("Expected json like key starting with {\"schema\":...", mapEntry.getKey().startsWith("{\"schema\":"));
            assertTrue("Expected json like value starting with {\"schema\":...", mapEntry.getValue().startsWith("{\"schema\":"));
        }

        jedis.close();
    }

}
