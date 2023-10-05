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

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/rh_integration/dbz-oracle");
    static final String DEFAULT_TAG = "21.3.0";
    private static final int ORACLE_DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;
    static final int ORACLE_PORT = 1521;
    private static final int APEX_HTTP_PORT = 8080;

    @Deprecated
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

    private void preconfigure() {
        withUsername("c##dbzuser").withPassword("dbz").withDatabaseName("ORCLCDB");
        this.waitStrategy = new WaitAllStrategy(WaitAllStrategy.Mode.WITH_INDIVIDUAL_TIMEOUTS_ONLY)
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
