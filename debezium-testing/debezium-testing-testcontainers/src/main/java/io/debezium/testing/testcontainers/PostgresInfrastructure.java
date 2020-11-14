/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Postgres Testcontainers infrastructure handling.
 */
public class PostgresInfrastructure {

    protected static final String POSTGRES_DEFAULT_IMAGE = "postgres:9.6.19";
    protected static final String POSTGRES_DEFAULT_DEBEZIUM_IMAGE = "debezium/example-postgres:1.3";

    protected static final Map<String, PostgresInfrastructure> postgresInfrastructures = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresInfrastructure.class);

    protected final String postgresImageName;
    protected final Network network = Network.newNetwork();
    private final PostgreSQLContainer<?> postgresContainer;

    private PostgresInfrastructure(String postgresImageName) {
        DockerImageName postgresDockerImageName = DockerImageName.parse(postgresImageName)
                .asCompatibleSubstituteFor("postgres");

        this.postgresImageName = postgresImageName;
        postgresContainer = new PostgreSQLContainer<>(postgresDockerImageName)
                .withNetwork(network)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withNetworkAliases("postgres");
    }

    public static PostgresInfrastructure getDebeziumPostgresInfrastructure() {
        return new PostgresInfrastructure(POSTGRES_DEFAULT_DEBEZIUM_IMAGE);
    }

    public static PostgresInfrastructure getPostgresInfrastructure() {
        return new PostgresInfrastructure(POSTGRES_DEFAULT_IMAGE);
    }

    public static PostgresInfrastructure getInfrastructure(String postgresImage) {
        if (postgresInfrastructures.containsKey(postgresImage)) {
            return postgresInfrastructures.get(postgresImage);
        }
        final PostgresInfrastructure infrastructure = new PostgresInfrastructure(postgresImage);
        postgresInfrastructures.put(postgresImage, infrastructure);
        return infrastructure;
    }

    public String getPostgresImageName() {
        return postgresImageName;
    }

    public PostgreSQLContainer<?> getPostgresContainer() {
        return postgresContainer;
    }

    public void startContainer() {
        Startables.deepStart(Stream.of(postgresContainer)).join();
    }

    public ConnectorConfiguration getPostgresConnectorConfiguration(int id, String... options) {
        final ConnectorConfiguration config = ConnectorConfiguration.forJdbcContainer(postgresContainer)
                .with("database.server.name", "dbserver" + id)
                .with("slot.name", "debezium_" + id);

        if (options != null && options.length > 0) {
            for (int i = 0; i < options.length; i += 2) {
                config.with(options[i], options[i + 1]);
            }
        }
        return config;
    }

}
