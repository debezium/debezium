/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.ImageNames;

/**
 * A Testcontainers database container for the StarRocks all-in-one image.
 */
public class StarRocksContainer<SELF extends StarRocksContainer<SELF>> extends JdbcDatabaseContainer<SELF> {

    public static final String NAME = "starrocks";
    public static final Integer STARROCKS_QUERY_PORT = 9030;

    // The all-in-one image can take several minutes to become writable when it starts
    // alongside other containers; this timeout also bounds the readiness probe loop.
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(10);

    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "";

    private String databaseName = "test";
    private String username = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;

    public StarRocksContainer() {
        this(ImageNames.STARROCKS_DOCKER_IMAGE_NAME);
    }

    public StarRocksContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public StarRocksContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(ImageNames.STARROCKS_DOCKER_IMAGE_NAME);
        addExposedPort(STARROCKS_QUERY_PORT);
        withStartupTimeout(STARTUP_TIMEOUT);
        withStartupTimeoutSeconds((int) STARTUP_TIMEOUT.toSeconds());
    }

    @Override
    protected void configure() {
        // Without a database qualifier the driver iterates every database for JDBC metadata
        // lookups, which StarRocks answers with analyzing errors for non-matching databases.
        withUrlParam("nullDatabaseMeansCurrent", "true");
        setStartupAttempts(1);
    }

    @Override
    protected void waitUntilContainerStarted() {
        // The all-in-one image starts a frontend and a backend node; the frontend accepts
        // connections before the backend has registered, so wait until the backend reports
        // itself alive and create the test database before the JDBC readiness probe (which
        // connects to getJdbcUrl(), i.e. the "/<databaseName>" schema) runs.
        final String baseUrl = "jdbc:starrocks://" + getHost() + ":" + getMappedPort(STARROCKS_QUERY_PORT)
                + "/" + constructUrlParameters("?", "&");
        Unreliables.retryUntilSuccess((int) STARTUP_TIMEOUT.toSeconds(), TimeUnit.SECONDS, () -> {
            try (Connection connection = DriverManager.getConnection(baseUrl, getUsername(), getPassword());
                    Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SHOW BACKENDS")) {
                    if (!rs.next() || !Boolean.parseBoolean(rs.getString("Alive"))) {
                        throw new IllegalStateException("StarRocks backend is not alive yet");
                    }
                }
                statement.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
                // An alive backend only becomes eligible for tablet placement once the frontend
                // has received its first disk report; probe with a real table creation so tests
                // cannot start before tables can actually be created.
                statement.execute("CREATE TABLE IF NOT EXISTS " + databaseName + ".readiness_probe (id int NOT NULL) "
                        + "PRIMARY KEY (id) DISTRIBUTED BY HASH (id)");
                statement.execute("DROP TABLE IF EXISTS " + databaseName + ".readiness_probe");
            }
            return null;
        });
        super.waitUntilContainerStarted();
    }

    @Override
    public String getDriverClassName() {
        return "com.starrocks.cj.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        final String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:starrocks://" + getHost() + ":" + getMappedPort(STARROCKS_QUERY_PORT) + "/" + databaseName + additionalUrlParams;
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
