/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.sqlserver.deployment;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.CI_CONTAINER_STARTUP_TIME;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class SqlServerResource {

    private static final MSSQLServerContainer<?> SQL_SERVER_CONTAINER = new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server:2019-latest"))
            .withNetworkAliases("sqlserver")
            .withEnv("SA_PASSWORD", "Password!")
            .withEnv("MSSQL_PID", "Standard")
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withPassword("Password!")
            .withInitScript("initialize-sqlserver-database.sql")
            .acceptLicense()
            .waitingFor(new LogMessageWaitStrategy()
                    .withRegEx(".*SQL Server is now ready for client connections\\..*\\s")
                    .withTimes(1)
                    .withStartupTimeout(Duration.of(CI_CONTAINER_STARTUP_TIME * 3, ChronoUnit.SECONDS)))
            .withStartupCheckStrategy(new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)))
            .withConnectTimeoutSeconds(300);

    public void start() {
        SQL_SERVER_CONTAINER.start();

        System.setProperty("SQLSERVER_JDBC", SQL_SERVER_CONTAINER.getJdbcUrl());
        System.setProperty("SQLSERVER_PASSWORD", SQL_SERVER_CONTAINER.getPassword());
        System.setProperty("SQLSERVER_USERNAME", SQL_SERVER_CONTAINER.getUsername());
    }

    public void stop() {
        SQL_SERVER_CONTAINER.stop();
    }
}
