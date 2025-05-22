/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class DatabaseTestResource implements QuarkusTestResourceLifecycleManager {
    private static final String POSTGRES_IMAGE = "quay.io/debezium/postgres:15";

    private static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(POSTGRES_DOCKER_IMAGE_NAME)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
            .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
            .withEnv("LANG", "en_US.utf8")
            .withUsername("postgres")
            .withPassword("postgres")
            .withDatabaseName("postgres")
            .withInitScript("init.sql")
            .withStartupTimeout(Duration.ofSeconds(30));

    @Override
    public Map<String, String> start() {
        try {
            postgresContainer.start();

            return Map.of(
                    "quarkus.datasource.db-kind", "postgresql",
                    "quarkus.debezium.database.hostname", postgresContainer.getHost(),
                    "quarkus.debezium.database.user", postgresContainer.getUsername(),
                    "quarkus.debezium.database.password", postgresContainer.getPassword(),
                    "quarkus.debezium.database.dbname", postgresContainer.getDatabaseName(),
                    "quarkus.debezium.database.port", postgresContainer.getMappedPort(5432).toString());
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
        catch (Exception ignore) {
        }
    }
}
