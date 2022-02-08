/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import java.time.Duration;

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

    private Long getStreamLength(String streamName, int expectedLength) {
        Awaitility.await().atMost(Duration.ofSeconds(RedisTestConfigSource.waitForSeconds())).until(() -> {
            return jedis.xlen(streamName) == expectedLength;
        });

        return jedis.xlen(streamName);
    }

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis
    */
    @Test
    public void testRedisStream() throws Exception {
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        Long streamLength = getStreamLength(STREAM_NAME, MESSAGE_COUNT);
        assertTrue("Redis Basic Stream Test Failed", streamLength == MESSAGE_COUNT);
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

        Long streamLength = getStreamLength(STREAM_NAME, MESSAGE_COUNT);

        Testing.print("Entries in " + STREAM_NAME + ":" + streamLength);
        assertTrue("Redis Connection Test Failed", streamLength == MESSAGE_COUNT);
    }

    /**
    * Test retry mechanism when encountering Redis Out of Memory:
    * 1. Simulate a Redis OOM by setting its max memory to 1M
    * 2. Create a new table named redis_test2 in PostgreSQL and insert 10 records to it
    * 3. Then, delete all the records in this table and expect the stream to contain 30 records 
    *    (10 inserted before + 20 as result of this deletion including the tombstone events)
    * 4. Insert 22 records to redis_test2 table and sleep for 1 second to simulate Redis OOM
    * 5. Delete the stream and expect those 22 records to be inserted to it as there's enough memory to complete this operation
    */
    @Test
    @FixFor("DBZ-4510")
    public void testRedisOOMRetry() throws Exception {
        final String STREAM_NAME = "testc.inventory.redis_test2";
        final int FIRST_BATCH_SIZE = 10;
        final int EXPECTED_STREAM_LENGTH_AFTER_DELETION = 30; // Every delete change record is followed by a tombstone event
        final int SECOND_BATCH_SIZE = 22;
        final String INSERT_SQL = "INSERT INTO inventory.redis_test2 (id,first_name,last_name) " +
                "SELECT LEFT(i::text, 10), RANDOM()::text, RANDOM()::text FROM generate_series(1,%d) s(i)";

        Testing.print("Setting Redis' maxmemory to 2M");
        jedis.configSet("maxmemory", "1M");

        PostgresConnection connection = getPostgresConnection();
        connection.execute("CREATE TABLE inventory.redis_test2 " +
                "(id VARCHAR(100) PRIMARY KEY, " +
                "first_name VARCHAR(100), " +
                "last_name VARCHAR(100))",
                String.format(INSERT_SQL, FIRST_BATCH_SIZE));

        Long streamLengthAfterInserts = getStreamLength(STREAM_NAME, FIRST_BATCH_SIZE);
        Testing.print("Entries in " + STREAM_NAME + ":" + streamLengthAfterInserts);

        connection.execute("DELETE FROM inventory.redis_test2");
        Long streamLengthAfterDeletion = getStreamLength(STREAM_NAME, EXPECTED_STREAM_LENGTH_AFTER_DELETION);
        Testing.print("Entries in " + STREAM_NAME + ":" + streamLengthAfterDeletion);

        connection.execute(String.format(INSERT_SQL, SECOND_BATCH_SIZE));
        connection.close();

        Thread.sleep(1000);

        Testing.print("Deleting stream in order to free memory");
        jedis.del(STREAM_NAME);

        Long streamLength = getStreamLength(STREAM_NAME, SECOND_BATCH_SIZE);

        Testing.print("Entries in " + STREAM_NAME + ":" + streamLength);
        jedis.configSet("maxmemory", "0");

        assertTrue("Redis OOM Test Failed", streamLength == SECOND_BATCH_SIZE);
    }
}
