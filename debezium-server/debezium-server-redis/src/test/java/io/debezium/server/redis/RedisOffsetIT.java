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

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.server.TestConfigSource;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Integration test that verifies reading and writing offsets from Redis key value store
 *
 * @author Oren Elias
 */
@QuarkusIntegrationTest
@TestProfile(RedisOffsetTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)

public class RedisOffsetIT {
    private static final int MESSAGE_COUNT = 4;
    private static final String STREAM_NAME = "testc.inventory.customers";

    protected static Jedis jedis;

    private PostgresConnection getPostgresConnection() {
        return new PostgresConnection(JdbcConfiguration.create()
                .with("user", PostgresTestResourceLifecycleManager.POSTGRES_USER)
                .with("password", PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD)
                .with("dbname", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME)
                .with("hostname", PostgresTestResourceLifecycleManager.POSTGRES_HOST)
                .with("port", PostgresTestResourceLifecycleManager.getContainer().getMappedPort(PostgresTestResourceLifecycleManager.POSTGRES_PORT))
                .build(), "Debezium Redis Test");
    }

    @Test
    @FixFor("DBZ-4509")
    public void testRedisStream() throws Exception {
        jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final List<StreamEntry> entries = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            final List<StreamEntry> response = jedis.xrange(STREAM_NAME, (StreamEntryID) null, (StreamEntryID) null, MESSAGE_COUNT);
            entries.addAll(response);
            return entries.size() >= MESSAGE_COUNT;
        });

        Map<String, String> redisOffsets = jedis.hgetAll("metadata:debezium:offsets");
        Assertions.assertThat(redisOffsets.size() > 0).isTrue();
    }

    /**
    * Test retry mechanism when encountering Redis connectivity issues:
    * 1. Make Redis to be unavailable while the server is up
    * 2. Create a new table named redis_test in PostgreSQL and insert 5 records to it
    * 3. Bring Redis up again and make sure the offsets have been written successfully
    */
    @Test
    @FixFor("DBZ-4509")
    public void testRedisConnectionRetry() throws Exception {
        Testing.Print.enable();

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        // wait until the offsets are written for the first time
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map<String, String> redisOffsets = jedis.hgetAll("metadata:debezium:offsets");
            return redisOffsets.size() > 0;
        });

        // clear the offsets key
        jedis.del("metadata:debezium:offsets");

        // pause container
        Testing.print("Pausing container");
        RedisTestResourceLifecycleManager.pause();

        final PostgresConnection connection = getPostgresConnection();
        Testing.print("Creating new redis_test table and inserting 5 records to it");
        connection.execute(
                "CREATE TABLE inventory.redis_test (id INT PRIMARY KEY)",
                "INSERT INTO inventory.redis_test VALUES (1)",
                "INSERT INTO inventory.redis_test VALUES (2)",
                "INSERT INTO inventory.redis_test VALUES (3)",
                "INSERT INTO inventory.redis_test VALUES (4)",
                "INSERT INTO inventory.redis_test VALUES (5)");
        connection.close();

        Testing.print("Sleeping for 2 seconds to flush records");
        Thread.sleep(2000);
        Testing.print("Unpausing container");

        RedisTestResourceLifecycleManager.unpause();
        Testing.print("Sleeping for 2 seconds to reconnect to redis and write offset");

        // wait until the offsets are re-written
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            Map<String, String> redisOffsets = jedis.hgetAll("metadata:debezium:offsets");
            return redisOffsets.size() > 0;
        });
        Map<String, String> redisOffsets = jedis.hgetAll("metadata:debezium:offsets");
        jedis.close();
        Assertions.assertThat(redisOffsets.size() > 0).isTrue();
    }

}
