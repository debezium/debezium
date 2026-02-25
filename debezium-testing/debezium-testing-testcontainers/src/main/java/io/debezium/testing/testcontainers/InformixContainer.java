/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.time.Duration;
import java.util.concurrent.Future;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

public class InformixContainer extends JdbcDatabaseContainer<InformixContainer> {

    public static final String NAME = "informix";

    private static final String FALLBACK_INFORMIX_VERSION = "14";
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/rh_integration/dbz-informix");
    public static final String DEFAULT_TAG = parameterWithDefault(System.getProperty("version.informix.server"), FALLBACK_INFORMIX_VERSION);
    private static final String INFORMIX_USERNAME = parameterWithDefault(System.getProperty("database.username"), "informix");
    private static final String INFORMIX_PASSWORD = parameterWithDefault(System.getProperty("database.password"), "in4mix");
    public static final String INFORMIX_DBNAME = System.getProperty("test.database.informix.dbz.dbname", "testdb");

    public static final int INFORMIX_PORT = 9088;
    private static final int INFORMIX_DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;

    public InformixContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public InformixContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public InformixContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        preconfigure();
    }

    public InformixContainer(Future<String> dockerImageName) {
        super(dockerImageName);
        preconfigure();
    }

    private static String parameterWithDefault(String value, String defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }

    public static WaitAllStrategy getWaitStrategyForVersion(String version) {
        WaitAllStrategy waitStrategy = new WaitAllStrategy(WaitAllStrategy.Mode.WITH_OUTER_TIMEOUT)
                .withStartupTimeout(Duration.ofSeconds(INFORMIX_DEFAULT_STARTUP_TIMEOUT_SECONDS));

        if ("12".equals(version)) {
            waitStrategy.withStrategy(new LogMessageWaitStrategy()
                    .withRegEx(".*Logical Log \\d+ Complete.*\\s")
                    .withTimes(1)
                    .withStartupTimeout(Duration.ofSeconds(INFORMIX_DEFAULT_STARTUP_TIMEOUT_SECONDS)));
        }
        else {
            waitStrategy.withStrategy(new LogMessageWaitStrategy()
                    .withRegEx(".*SCHAPI: Started \\d+ dbWorker threads.*\\s")
                    .withTimes(1)
                    .withStartupTimeout(Duration.ofSeconds(INFORMIX_DEFAULT_STARTUP_TIMEOUT_SECONDS)));
        }

        return waitStrategy;
    }

    private void preconfigure() {
        addExposedPort(INFORMIX_PORT);
        withEnv("LICENSE", "accept");
        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        // WaitStrategy needs to be set like this, otherwise is being ignored for JdbcDatabaseContainer
        // Check: https://github.com/testcontainers/testcontainers-java/issues/2994
        this.waitStrategy = getWaitStrategyForVersion(DEFAULT_TAG);
    }

    public String getDriverClassName() {
        return "com.informix.jdbc.IfxDriver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:informix-sqli://" + this.getHost() + ":" + this.getMappedPort(INFORMIX_PORT) + "/" + INFORMIX_DBNAME;
    }

    @Override
    public String getUsername() {
        return INFORMIX_USERNAME;
    }

    @Override
    public String getPassword() {
        return INFORMIX_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1 FROM informix.systables;";
    }
}
