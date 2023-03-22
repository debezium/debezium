/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for PostgreSQL.
 *
 * @author Chris Cranford
 */
public class PostgresSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("postgres");

    @SuppressWarnings("resource")
    public PostgresSinkDatabaseContextProvider() {
        super(SinkType.POSTGRES,
                new PostgreSQLContainer<>(IMAGE_NAME)
                        .withNetwork(Network.newNetwork())
                        .withDatabaseName("test"));
    }

}
