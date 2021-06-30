/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.it;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * @author Chris Cranford
 */
public class DatabaseTestResource implements QuarkusTestResourceLifecycleManager {

    private static final String POSTGRES_IMAGE = "debezium/postgres:11";

    private static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    private static PostgreSQLContainer<?> postgresContainer;

    @Override
    @SuppressWarnings("resource") // closed in stop()
    public Map<String, String> start() {
        try {
            postgresContainer = new PostgreSQLContainer<>(POSTGRES_DOCKER_IMAGE_NAME)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withDatabaseName("postgres")
                    .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
                    .withEnv("LANG", "en_US.utf8")
                    .withStartupTimeout(Duration.ofSeconds(30));

            postgresContainer.start();
            return Collections.singletonMap("quarkus.datasource.jdbc.url", postgresContainer.getJdbcUrl());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            if (postgresContainer != null) {
                postgresContainer.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }
}
