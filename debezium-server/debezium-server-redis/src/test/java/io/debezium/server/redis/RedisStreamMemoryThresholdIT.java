/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

@QuarkusIntegrationTest
@TestProfile(RedisStreamMemoryThresholdTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamMemoryThresholdIT {

    @Test
    public void testRedisMemoryThreshold() throws Exception {
        Testing.Print.enable();

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final String STREAM_NAME = "testc.inventory.redis_test2";
        final int TOTAL_RECORDS = 50;

        Testing.print("Setting Redis' maxmemory to 1M");
        jedis.configSet("maxmemory", "1M");

        PostgresConnection connection = TestUtils.getPostgresConnection();
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
        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, TOTAL_RECORDS);

        long streamLength = jedis.xlen(STREAM_NAME);
        assertTrue("Redis Memory Threshold Test Failed", streamLength == TOTAL_RECORDS);
    }

}
