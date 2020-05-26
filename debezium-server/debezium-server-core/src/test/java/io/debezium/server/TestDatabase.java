/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * @author Jiri Pechanec
 */
public class TestDatabase {

    public static final String POSTGRES_USER = "postgres";
    public static final String POSTGRES_PASSWORD = "postgres";
    public static final String POSTGRES_DBNAME = "postgres";
    public static final String POSTGRES_IMAGE = "debezium/example-postgres";
    public static final String POSTGRES_HOST = "localhost";
    public static final Integer POSTGRES_PORT = 5432;

    private GenericContainer container;

    public void start() {
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
        }
        catch (Exception e) {
            Assertions.fail(e);
        }
    }

    public String getIp() {
        return POSTGRES_HOST;
    }

    public int getPort() {
        return POSTGRES_PORT;
    }

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
