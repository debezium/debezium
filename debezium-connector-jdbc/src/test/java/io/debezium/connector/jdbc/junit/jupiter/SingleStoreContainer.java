/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.ImageNames;

/**
 * A Testcontainers database container for the SingleStore Dev Image.
 */
public class SingleStoreContainer<SELF extends SingleStoreContainer<SELF>> extends JdbcDatabaseContainer<SELF> {

    public static final String NAME = "singlestore";
    public static final Integer SINGLESTORE_PORT = 3306;

    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "root";

    private String databaseName = "test";
    private String username = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;

    public SingleStoreContainer() {
        this(ImageNames.SINGLESTORE_DOCKER_IMAGE_NAME);
    }

    public SingleStoreContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public SingleStoreContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(ImageNames.SINGLESTORE_DOCKER_IMAGE_NAME);
        addExposedPort(SINGLESTORE_PORT);
        withStartupTimeout(Duration.ofMinutes(5));
    }

    @Override
    protected void configure() {
        addEnv("DATABASE_NAME", databaseName);
        addEnv("ROOT_PASSWORD", password);
        setStartupAttempts(1);
    }

    @Override
    protected void waitUntilContainerStarted() {
        // The SingleStore dev image does not auto-create a database from the DATABASE_NAME environment
        // variable, so create it before the JDBC readiness probe (which connects to getJdbcUrl(), i.e.
        // the "/<databaseName>" schema) runs and would otherwise fail with "Unknown database".
        final String baseUrl = "jdbc:singlestore://" + getHost() + ":" + getMappedPort(SINGLESTORE_PORT)
                + "/" + constructUrlParameters("?", "&");
        Unreliables.retryUntilSuccess(getConnectTimeoutSeconds(), TimeUnit.SECONDS, () -> {
            try (Connection connection = DriverManager.getConnection(baseUrl, getUsername(), getPassword());
                    Statement statement = connection.createStatement()) {
                statement.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
            }
            return null;
        });
        super.waitUntilContainerStarted();
    }

    @Override
    public String getDriverClassName() {
        return "com.singlestore.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        final String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:singlestore://" + getHost() + ":" + getMappedPort(SINGLESTORE_PORT) + "/" + databaseName + additionalUrlParams;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    public SELF withDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return self();
    }

    @Override
    public SELF withUsername(String username) {
        this.username = username;
        return self();
    }

    @Override
    public SELF withPassword(String password) {
        this.password = password;
        return self();
    }
}
