/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * @author Chris Cranford
 */
public class DatabaseTestResource implements QuarkusTestResourceLifecycleManager {
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    private static final String POSTGRES_DBNAME = "postgres";
    private static final String POSTGRES_IMAGE = "debezium/postgres:9.6";
    private static final Integer POSTGRES_PORT = 5432;

    private GenericContainer container;

    @Override
    public Map<String, String> start() {
        try {

            container = new FixedHostPortGenericContainer(POSTGRES_IMAGE)
                    .withFixedExposedPort(POSTGRES_PORT, POSTGRES_PORT)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
                    .withEnv("POSTGRES_USER", POSTGRES_USER)
                    .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
                    .withEnv("POSTGRES_DB", POSTGRES_DBNAME)
                    .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
                    .withEnv("LANG", "en_US.utf8")
                    .withStartupTimeout(Duration.ofSeconds(30));

            container.start();

            return Collections.emptyMap();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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
