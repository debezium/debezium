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

import javax.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
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
 * Integration tests that verify basic reading from PostgreSQL database and writing to Redis stream
 * and retry mechanism in case of connectivity issues or OOM in Redis
 *
 * @author M Sazzadul Hoque
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamIT {

    @ConfigProperty(name = "debezium.source.database.hostname")
    String dbHostname;

    @ConfigProperty(name = "debezium.source.database.port")
    String dbPort;

    @ConfigProperty(name = "debezium.source.database.user")
    String dbUser;

    @ConfigProperty(name = "debezium.source.database.password")
    String dbPassword;

    @ConfigProperty(name = "debezium.source.database.dbname")
    String dbName;

    protected static Jedis jedis;

    {
        Testing.Files.delete(RedisTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RedisTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Print.enable();
        jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    private PostgresConnection getPostgresConnection() {
        return new PostgresConnection(Configuration.create()
                .with("hostname", dbHostname)
                .with("port", dbPort)
                .with("user", dbUser)
                .with("password", dbPassword)
                .with("dbname", dbName)
                .build());
    }

    private List<StreamEntry> getStreamElements(String streamName, int messageCount) {
        final List<StreamEntry> entries = new ArrayList<>();

        Awaitility.await().atMost(Duration.ofSeconds(RedisTestConfigSource.waitForSeconds())).until(() -> {
            final List<StreamEntry> response = jedis.xrange(streamName, null, null, messageCount);
            entries.clear();
            entries.addAll(response);
            return entries.size() == messageCount;
        });

        return entries;
    }

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis
    */
    @Test
    public void testRedisStream() throws Exception {
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        final List<StreamEntry> entries = getStreamElements(STREAM_NAME, MESSAGE_COUNT);
        assertTrue("Redis Basic Stream Test Failed", entries.size() == MESSAGE_COUNT);
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
        final int MESSAGE_COUNT = 5;
        final String STREAM_NAME = "testc.inventory.redis_test";
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

        Testing.print("Sleeping for 5 seconds to simulate no connection errors");
        Thread.sleep(5000);
        Testing.print("Unpausing container");
        RedisTestResourceLifecycleManager.unpause();

        final List<StreamEntry> entries = getStreamElements(STREAM_NAME, MESSAGE_COUNT);

        Testing.print("Entries in " + STREAM_NAME + ":" + entries.size());
        assertTrue("Redis Connection Test Failed", entries.size() == MESSAGE_COUNT);
    }

    /**
    * Test retry mechanism when encountering Redis Out of Memory:
    * 1. Simulate a Redis OOM by setting its max memory to 1M.
    * 2. Create a new table named redis_test2 in PostgreSQL and insert 1000 records to it.
    * 3. As result, after inserting ~22 records, Redis runs OOM.
    * 4. Revert max memory setting so Redis is no longer in OOM and make sure all 100 records have been streamed successfully.
     */
    @Test
    @FixFor("DBZ-4510")
    public void testRedisOOMRetry() throws Exception {
        final int MESSAGE_COUNT = 100;
        final String STREAM_NAME = "testc.inventory.redis_test2";
        Testing.print("Setting Redis' maxmemory to 1M");
        jedis.configSet("maxmemory", "1M");
        final PostgresConnection connection = getPostgresConnection();
        Testing.print("Creating new table redis_test2 and inserting 100 records to it");
        connection.execute("CREATE TABLE inventory.redis_test2 " +
                "(id VARCHAR(100) PRIMARY KEY, " +
                "first_name VARCHAR(100), " +
                "last_name VARCHAR(100))",
                String.format("INSERT INTO inventory.redis_test2 (id,first_name,last_name) " +
                        "SELECT LEFT(i::text, 10), RANDOM()::text, RANDOM()::text FROM generate_series(1,%d) s(i)",
                        MESSAGE_COUNT));
        connection.close();

        final List<StreamEntry> entriesWhenOOM = getStreamElements(STREAM_NAME, 22);

        Testing.print("Stream size in OOM:" + entriesWhenOOM.size());
        Testing.print("Reverting Redis' maxmemory");
        jedis.configSet("maxmemory", "0");

        final List<StreamEntry> entries = getStreamElements(STREAM_NAME, MESSAGE_COUNT);

        Testing.print("Entries in " + STREAM_NAME + ":" + entries.size());
        assertTrue("Redis OOM Test Failed", entries.size() == MESSAGE_COUNT);
    }
}
