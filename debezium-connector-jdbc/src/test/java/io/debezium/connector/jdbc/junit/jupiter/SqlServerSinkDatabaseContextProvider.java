/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.testing.testcontainers.ImageNames;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for SQL Server.
 *
 * @author Chris Cranford
 */
public class SqlServerSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    private static final DockerImageName IMAGE_NAME = ImageNames.SQLSERVER_22_IMAGE_NAME;

    @SuppressWarnings("resource")
    public SqlServerSinkDatabaseContextProvider() {
        super(SinkType.SQLSERVER,
                new MSSQLServerContainer<>(IMAGE_NAME)
                        .withPassword("Debezium1!")
                        .withEnv("MSSQL_AGENT_ENABLED", "true")
                        .withEnv("MSSQL_PID", "Standard")
                        .withNetwork(Network.SHARED)
                        .withInitScript("database-init-scripts/sqlserver-init.sql")
                        .acceptLicense()
                        .withEnv("TZ", TestHelper.getSinkTimeZone()));
    }

}
