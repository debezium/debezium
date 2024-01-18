/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.testing.testcontainers.util.ContainerImageVersions;
import io.debezium.util.Testing;

/**
 * Integration test for {@link PostgresConnector} using an {@link EmbeddedEngine} and Testcontainers infrastructure for when Postgres is shutdown during streaming
 */
public class PostgresShutdownIT extends AbstractConnectorTest {

    /*
     * Specific tests that need to extend the initial DDL set should do it in a form of
     * TestHelper.execute(SETUP_TABLES_STMT + ADDITIONAL_STATEMENTS)
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresShutdownIT.class);

    private static final String POSTGRES_IMAGE = ContainerImageVersions.getStableImage("quay.io/debezium/example-postgres");

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));" +
            "CREATE TABLE s1.heartbeat (ts TIMESTAMP WITH TIME ZONE PRIMARY KEY);" +
            "INSERT INTO s1.heartbeat (ts) VALUES (NOW());";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;

    private static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    public static PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(POSTGRES_DOCKER_IMAGE_NAME)
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withNetworkAliases("postgres");

    private String oldContainerPort;

    @Before
    public void setUp() {
        postgresContainer.start();
        oldContainerPort = System.getProperty("database.port", "5432");
        System.setProperty("database.port", String.valueOf(postgresContainer.getMappedPort(5432)));
        try {
            TestHelper.dropAllSchemas();
        }
        catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
        initializeConnectorTestFramework();
    }

    @After
    public void tearDown() {
        stopConnector();
        postgresContainer.stop();
        System.setProperty("database.port", oldContainerPort);
    }

    @Test
    @FixFor("DBZ-2617")
    public void shouldStopOnPostgresFastShutdown() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        final int recordCount = 100;

        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STMT);
        }
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT, postgresContainer.getMappedPort(5432))
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(Heartbeat.HEARTBEAT_INTERVAL, 500)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY, "UPDATE s1.heartbeat SET ts=NOW();");

        Testing.Print.enable();
        PostgresConnection postgresConnection = TestHelper.create();
        String initialHeartbeat = postgresConnection.queryAndMap(
                "SELECT ts FROM s1.heartbeat;",
                postgresConnection.singleResultMapper(rs -> rs.getString("ts"), "Could not fetch keepalive info"));
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        logger.info("Waiting for heartbeats...");
        Awaitility.await()
                .pollInterval(250, TimeUnit.MILLISECONDS)
                .atMost(5 * TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .until(() -> !initialHeartbeat.equals(postgresConnection.queryAndMap(
                        "SELECT ts FROM s1.heartbeat;",
                        postgresConnection.singleResultMapper(rs -> rs.getString("ts"), "Could not fetch keepalive info"))));
        logger.info("INTIAL Heartbeat: " + initialHeartbeat + " ; CURRENT heartbeat: "
                + postgresConnection.queryAndMap(
                        "SELECT ts FROM s1.heartbeat;",
                        postgresConnection.singleResultMapper(rs -> rs.getString("ts"), "Could not fetch keepalive info")));

        logger.info("Execute Postgres shutdown...");
        Container.ExecResult result = postgresContainer
                .execInContainer("su", "-", "postgres", "-c", "/usr/lib/postgresql/*/bin/pg_ctl -m fast -D /var/lib/postgresql/data stop");
        logger.info(result.toString());

        logger.info("Waiting for Postgres to shut down...");
        waitForPostgresShutdown();

        logger.info("Waiting for connector to shut down...");
        waitForConnectorShutdown("postgres", TestHelper.TEST_SERVER);
    }

    private void waitForPostgresShutdown() {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(60 * TestHelper.waitTimeForRecords(), TimeUnit.SECONDS)
                .until(() -> !postgresContainer.isRunning());
    }
}
