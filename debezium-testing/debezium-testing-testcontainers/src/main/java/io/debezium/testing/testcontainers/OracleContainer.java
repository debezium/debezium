/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Future;

import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

public class OracleContainer extends org.testcontainers.containers.OracleContainer {

    private static final String FALLBACK_ORACLE_SERVER_VERSION = "21.3.0";
    public static final String DEFAULT_TAG = parameterWithDefault(System.getProperty("version.oracle.server"), FALLBACK_ORACLE_SERVER_VERSION);
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/rh_integration/dbz-oracle");
    public final String ORACLE_DBNAME = parameterWithDefault(System.getProperty("database.dbname"), "ORCLCDB");
    public final String ORACLE_PDB_NAME = parameterWithDefault(System.getProperty("database.pdb.name"), "ORCLPDB1");
    private static final String ORACLE_USERNAME = parameterWithDefault(System.getProperty("database.username"), "debezium");
    private static final String ORACLE_PASSWORD = parameterWithDefault(System.getProperty("database.password"), "dbz");
    public static final int ORACLE_PORT = 1521;
    private static final int ORACLE_DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;
    private static final int APEX_HTTP_PORT = 8080;

    public OracleContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG).asCompatibleSubstituteFor("gvenzl/oracle-xe"));
    }

    public OracleContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName).asCompatibleSubstituteFor("gvenzl/oracle-xe"));
    }

    public OracleContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        preconfigure();
    }

    public OracleContainer(Future<String> dockerImageName) {
        super(dockerImageName);
        preconfigure();
    }

    private static String parameterWithDefault(String value, String defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }

    private void preconfigure() {
        withUsername(ORACLE_USERNAME).withPassword(ORACLE_PASSWORD).withDatabaseName(ORACLE_DBNAME);
        this.waitStrategy = new WaitAllStrategy(WaitAllStrategy.Mode.WITH_OUTER_TIMEOUT)
                .withStartupTimeout(Duration.of(ORACLE_DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .withStrategy(new LogMessageWaitStrategy()
                        .withRegEx(".*DATABASE IS READY TO USE!.*\\s")
                        .withTimes(1)
                        .withStartupTimeout(Duration.of(ORACLE_DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS)))
                .withStrategy(new LogMessageWaitStrategy()
                        .withRegEx(".*DONE: Executing user defined scripts.*\\s")
                        .withTimes(1)
                        .withStartupTimeout(Duration.of(ORACLE_DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS)));
        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        addExposedPorts(ORACLE_PORT, APEX_HTTP_PORT);
    }

    public String getDriverClassName() {
        return "oracle.jdbc.OracleDriver";
    }

}
