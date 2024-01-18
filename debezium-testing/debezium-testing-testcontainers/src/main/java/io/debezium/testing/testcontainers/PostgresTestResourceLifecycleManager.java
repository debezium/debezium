/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * @author Jiri Pechanec
 */
public class PostgresTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final String POSTGRES_USER = "postgres";
    public static final String POSTGRES_PASSWORD = "postgres";
    public static final String POSTGRES_DBNAME = "postgres";
    public static final String POSTGRES_IMAGE = "quay.io/debezium/example-postgres";
    public static final String POSTGRES_HOST = "localhost";
    public static final Integer POSTGRES_PORT = 5432;

    private static final GenericContainer<?> container = new GenericContainer<>(POSTGRES_IMAGE)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
            .withEnv("POSTGRES_USER", POSTGRES_USER)
            .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .withEnv("POSTGRES_DB", POSTGRES_DBNAME)
            .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
            .withEnv("LANG", "en_US.utf8")
            .withExposedPorts(POSTGRES_PORT)
            .withStartupTimeout(Duration.ofSeconds(30));

    public static GenericContainer<?> getContainer() {
        return container;
    }

    @Override
    public Map<String, String> start() {
        container.start();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.source.database.hostname", POSTGRES_HOST);
        params.put("debezium.source.database.port", container.getMappedPort(POSTGRES_PORT).toString());
        params.put("debezium.source.database.user", POSTGRES_USER);
        params.put("debezium.source.database.password", POSTGRES_PASSWORD);
        params.put("debezium.source.database.dbname", POSTGRES_DBNAME);
        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }
}
