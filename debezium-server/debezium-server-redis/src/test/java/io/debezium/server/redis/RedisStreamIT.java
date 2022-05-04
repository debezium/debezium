/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.time.Duration;

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

/**
 * Integration tests that verify basic reading from PostgreSQL database and writing to Redis stream
 * and retry mechanism in case of connectivity issues or OOM in Redis
 *
 * @author M Sazzadul Hoque
 * @author Yossi Shirizli
 */
@QuarkusIntegrationTest
@TestProfile(RedisStreamTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamIT {

    private PostgresConnection getPostgresConnection() {
        return new PostgresConnection(JdbcConfiguration.create()
                .with("user", PostgresTestResourceLifecycleManager.POSTGRES_USER)
                .with("password", PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD)
                .with("dbname", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME)
                .with("hostname", PostgresTestResourceLifecycleManager.POSTGRES_HOST)
                .with("port", PostgresTestResourceLifecycleManager.getContainer().getMappedPort(PostgresTestResourceLifecycleManager.POSTGRES_PORT))
                .build(), "Debezium Redis Test");
    }

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
        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        Long streamLength = getStreamLength(jedis, STREAM_NAME, MESSAGE_COUNT);
        assertTrue("Redis Basic Stream Test Failed", streamLength == MESSAGE_COUNT);
        jedis.close();
    }

    /**
    * Test retry mechanism when encountering Redis connectivity issues:
    * 1. Make Redis to be unavailable while the server is up
    * 2. Create a new table named redis_test in PostgreSQL and insert 5 records to it
    * 3. Bring Redis up again and make sure these records have been streamed successfully
    */
    @Test
    @FixFor("DBZ-4510")
    public void testRedisConnectionRetry() throws Exception {
        Testing.Print.enable();

        final int MESSAGE_COUNT = 5;
        final String STREAM_NAME = "testc.inventory.redis_test";
        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
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

        Testing.print("Sleeping for 3 seconds to simulate no connection errors");
        Thread.sleep(3000);
        Testing.print("Unpausing container");
        RedisTestResourceLifecycleManager.unpause();
        Thread.sleep(2000);

        Long streamLength = jedis.xlen(STREAM_NAME);
        Testing.print("Entries in " + STREAM_NAME + ":" + streamLength);
        jedis.close();
        assertTrue("Redis Connection Test Failed", streamLength == MESSAGE_COUNT);
    }

    /**
    * Test retry mechanism when encountering Redis Out of Memory:
    * 1. Simulate a Redis OOM by setting its max memory to 1M
    * 2. Create a new table named redis_test2 in PostgreSQL and insert 50 records to it
    * 3. Sleep for 1 second to simulate Redis OOM (stream does not contain 50 records)
    * 4. Unlimit memory and verify that all 50 records have been streamed
    */
    @Test
    @FixFor("DBZ-4510")
    public void testRedisOOMRetry() throws Exception {
        Testing.Print.enable();

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final String STREAM_NAME = "testc.inventory.redis_test2";
        final int TOTAL_RECORDS = 50;

        Testing.print("Setting Redis' maxmemory to 1M");
        jedis.configSet("maxmemory", "1M");

        PostgresConnection connection = getPostgresConnection();
        connection.execute("CREATE TABLE inventory.redis_test2 " +
                "(id VARCHAR(100) PRIMARY KEY, " +
                "first_name VARCHAR(100), " +
                "last_name VARCHAR(100))");
        connection.execute(String.format("INSERT INTO inventory.redis_test2 (id,first_name,last_name) " +
                "SELECT LEFT(i::text, 10), RANDOM()::text, RANDOM()::text FROM generate_series(1,%d) s(i)", TOTAL_RECORDS));
        connection.commit();

        Thread.sleep(1000);
        Testing.print("Entries in " + STREAM_NAME + ":" + jedis.xlen(STREAM_NAME));
        assertTrue(jedis.xlen(STREAM_NAME) < TOTAL_RECORDS);

        Thread.sleep(1000);
        jedis.configSet("maxmemory", "0");
        Long streamLength = getStreamLength(jedis, STREAM_NAME, TOTAL_RECORDS);

        assertTrue("Redis OOM Test Failed", streamLength == TOTAL_RECORDS);
    }
}
