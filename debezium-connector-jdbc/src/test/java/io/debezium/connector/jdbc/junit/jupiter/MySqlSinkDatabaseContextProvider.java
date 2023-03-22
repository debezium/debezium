/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("mysql");

    @SuppressWarnings("resource")
    public MySqlSinkDatabaseContextProvider() {
        super(SinkType.MYSQL, new MySQLContainer<>(IMAGE_NAME)
                .withNetwork(Network.newNetwork())
                .withDatabaseName("test"));
    }

}
