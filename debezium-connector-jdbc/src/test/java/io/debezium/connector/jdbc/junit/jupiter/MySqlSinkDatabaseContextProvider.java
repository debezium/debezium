/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.TestHelper;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("mysql:8.2")
            .asCompatibleSubstituteFor("mysql");

    @SuppressWarnings("resource")
    public MySqlSinkDatabaseContextProvider() {
        super(SinkType.MYSQL, createContainer());
    }

    private static MySQLContainer createContainer() {
        MySQLContainer container = new MySQLContainer<>(IMAGE_NAME)
                .withNetwork(Network.newNetwork())
                .withDatabaseName("test")
                .withUsername("mysqluser")
                .withPassword("debezium")
                .withEnv("TZ", TestHelper.getSinkTimeZone());
        if (TestHelper.isConnectionTimeZoneUsed()) {
            container.withUrlParam("connectionTimeZone", TestHelper.getSinkTimeZone());
        }
        return container;
    }
}
