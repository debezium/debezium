/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.time.Duration;
import java.util.function.Supplier;

import org.awaitility.Awaitility;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.server.TestConfigSource;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

import redis.clients.jedis.Jedis;

public class TestUtils {

    private TestUtils() {
    }

    public static PostgresConnection getPostgresConnection() {
        return new PostgresConnection(JdbcConfiguration.create()
                .with("user", PostgresTestResourceLifecycleManager.POSTGRES_USER)
                .with("password", PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD)
                .with("dbname", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME)
                .with("hostname", PostgresTestResourceLifecycleManager.POSTGRES_HOST)
                .with("port", PostgresTestResourceLifecycleManager.getContainer().getMappedPort(PostgresTestResourceLifecycleManager.POSTGRES_PORT))
                .build(), "Debezium Redis Test");
    }

    public static void awaitStreamLengthGte(Jedis jedis, String streamName, int expectedLength) {
        waitBoolean(() -> jedis.xlen(streamName) >= expectedLength);
    }

    public static void awaitHashSizeGte(Jedis jedis, String hashName, int expectedSize) {
        waitBoolean(() -> jedis.hgetAll(hashName).size() >= expectedSize);
    }

    private static void waitBoolean(Supplier<Boolean> bool) {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            return Boolean.TRUE.equals(bool.get());
        });
    }

}
