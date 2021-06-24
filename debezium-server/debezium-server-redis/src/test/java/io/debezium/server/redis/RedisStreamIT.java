/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to Redis stream.
 *
 * @author M Sazzadul Hoque
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamIT {

    private static final int MESSAGE_COUNT = 4;
    private static final String STREAM_NAME = "testc.inventory.customers";

    protected static Jedis jedis;

    {
        Testing.Files.delete(RedisTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RedisTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Print.enable();

        jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));

        // clean-up STREAM_NAME
        jedis.del(STREAM_NAME);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testRedisStream() throws Exception {
        Testing.Print.enable();
        final List<StreamEntry> entries = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(RedisTestConfigSource.waitForSeconds())).until(() -> {
            final List<StreamEntry> response = jedis.xrange(STREAM_NAME, null, null, MESSAGE_COUNT);
            entries.addAll(response);
            return entries.size() >= MESSAGE_COUNT;
        });
        Assertions.assertThat(entries.size() >= MESSAGE_COUNT);
    }
}
