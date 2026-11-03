/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * An Oracle test container, by default based on the official Oracle Database Free image, with the
 * {@code debezium} user in the pluggable database and the {@code c##dbzuser} LogMiner user.
 *
 * The image can be overridden with the {@code oracle.image} system property. Images other than
 * Oracle Database Free, such as {@code quay.io/rh_integration/dbz-oracle}, are expected to use the
 * {@code ORCLCDB} and {@code ORCLPDB1} databases and to already provide the test users.
 *
 * @author Chris Cranford
 */
public class JdbcOracleContainer extends OracleContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcOracleContainer.class);

    public static final String DEFAULT_IMAGE_NAME = "container-registry.oracle.com/database/free:latest-lite";
    public static final String IMAGE_NAME_PROPERTY = "oracle.image";

    public static final String USERNAME = "debezium";
    public static final String PASSWORD = "dbz";

    private static final String FREE_IMAGE_MARKER = "/database/free";
    private static final String FREE_CDB_NAME = "FREE";
    private static final String FREE_PDB_NAME = "FREEPDB1";
    private static final String DBZ_ORACLE_CDB_NAME = "ORCLCDB";
    private static final String DBZ_ORACLE_PDB_NAME = "ORCLPDB1";

    private static final String SYS_PASSWORD = "top_secret";
    private static final String INIT_SCRIPT = "database-init-scripts/oracle-free-init.sql";
    private static final String STARTUP_SCRIPT_PATH = "/opt/oracle/scripts/startup/01-init.sql";
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(10);

    /**
     * Creates a new Oracle container.
     *
     * @param archiveLogEnabled whether an Oracle Database Free container should run in archive log mode
     */
    public JdbcOracleContainer(boolean archiveLogEnabled) {
        super(DockerImageName.parse(getImageName()).asCompatibleSubstituteFor("gvenzl/oracle-xe"));

        withUsername(USERNAME);
        withPassword(PASSWORD);
        withDatabaseName(getPdbName());

        if (isFreeEdition()) {
            withEnv("ORACLE_PWD", SYS_PASSWORD);
            withCopyFileToContainer(MountableFile.forClasspathResource(INIT_SCRIPT), STARTUP_SCRIPT_PATH);
            // Startup script errors do not fail the container start, so log them to aid diagnosis
            withLogConsumer(new Slf4jLogConsumer(LOGGER));
            // The official image creates no application user, so wait for the init script to finish
            waitingFor(Wait.forLogMessage(".*DONE: Executing user defined scripts.*", 1)
                    .withStartupTimeout(STARTUP_TIMEOUT));

            if (archiveLogEnabled) {
                withEnv("ENABLE_ARCHIVELOG", "true");
            }
        }
    }

    public static String getImageName() {
        return System.getProperty(IMAGE_NAME_PROPERTY, DEFAULT_IMAGE_NAME);
    }

    public static boolean isFreeEdition() {
        return getImageName().contains(FREE_IMAGE_MARKER);
    }

    public static String getCdbName() {
        return isFreeEdition() ? FREE_CDB_NAME : DBZ_ORACLE_CDB_NAME;
    }

    public static String getPdbName() {
        return isFreeEdition() ? FREE_PDB_NAME : DBZ_ORACLE_PDB_NAME;
    }
}
