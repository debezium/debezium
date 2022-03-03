/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.time.Duration;
import java.util.List;

import javax.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnection.MySqlConnectionConfiguration;
import io.debezium.doc.FixFor;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.MySqlTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Integration test that verifies reading and writing database history from Redis key value store
 *
 * @author Oren Elias
 */
@QuarkusIntegrationTest
@TestProfile(RedisDatabaseHistoryTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)

public class RedisDatabaseHistoryIT {

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        Testing.Print.enable();
    }

    private static final String STREAM_NAME = "metadata:debezium:db_history";

    protected static Jedis jedis;

    @Test
    @FixFor("DBZ-4771")
    public void testDatabaseHistory() throws Exception {
        jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        Testing.Print.enable();
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            final long streamLength = jedis.xlen(STREAM_NAME);
            return streamLength > 0;
        });

        final long streamLength = jedis.xlen(STREAM_NAME);
        Assertions.assertThat(streamLength > 0).isTrue();
    }

    /**
    * Test retry mechanism when encountering Redis connectivity issues:
    * 1. Make Redis unavailable while the server is up
    * 2. Create a new table named redis_test in MySQL
    * 3. Bring Redis up again and make sure the database history has been written successfully
    */
    @Test
    @FixFor("DBZ-4509")
    public void testRedisConnectionRetry() throws Exception {
        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        // wait until the db history is written for the first time
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            final long streamLength = jedis.xlen(STREAM_NAME);
            return streamLength > 0;
        });

        // clear the key
        jedis.del(STREAM_NAME);

        // pause container
        Testing.print("Pausing container");
        RedisTestResourceLifecycleManager.pause();

        final MySqlConnection connection = getMySqlConnection();
        System.out.println(connection.config());
        connection.connect();
        Testing.print("Creating new redis_test table and inserting 5 records to it");
        connection.execute("CREATE TABLE inventory.redis_test (id INT PRIMARY KEY)");
        connection.close();

        Testing.print("Sleeping for 2 seconds to flush records");
        Thread.sleep(2000);
        Testing.print("Unpausing container");
        RedisTestResourceLifecycleManager.unpause();

        // wait until the db history is written for the first time
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            final long streamLength = jedis.xlen(STREAM_NAME);
            return streamLength > 0;
        });
        final List<StreamEntry> entries = jedis.xrange(STREAM_NAME, (StreamEntryID) null, (StreamEntryID) null);
        Assertions.assertThat(entries.size() == 1).isTrue();
        Assertions.assertThat(entries.get(0).getFields().get("schema")).contains("redis_test");
    }

    private MySqlConnection getMySqlConnection() {
        return new MySqlConnection(new MySqlConnectionConfiguration(Configuration.create()
                .with("database.user", MySqlTestResourceLifecycleManager.PRIVILEGED_USER)
                .with("database.password", MySqlTestResourceLifecycleManager.PRIVILEGED_PASSWORD)
                .with("database.dbname", MySqlTestResourceLifecycleManager.DBNAME)
                .with("database.hostname", MySqlTestResourceLifecycleManager.HOST)
                .with("database.port", MySqlTestResourceLifecycleManager.getContainer().getMappedPort(MySqlTestResourceLifecycleManager.PORT))
                .build()));
    }
}
