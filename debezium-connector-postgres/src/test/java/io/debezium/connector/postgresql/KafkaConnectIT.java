/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.sql.SQLException;
import java.util.stream.Stream;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

/**
 * Integration test for {@link PostgresConnector} using Kafka Connect with Testcontainers infrastructure
 */
public class KafkaConnectIT extends AbstractConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectIT.class);

    /*
     * Specific tests that need to extend the initial DDL set should do it in a form of
     * TestHelper.execute(SETUP_TABLES_STMT + ADDITIONAL_STATEMENTS)
     */
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

    private static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(DEFAULT_DEBEZIUM_POSTGRES_EXAMPLE_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    private static final org.testcontainers.containers.Network NETWORK = org.testcontainers.containers.Network.newNetwork();

    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.3"))
            .withNetwork(NETWORK);

    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER = new PostgreSQLContainer<>(POSTGRES_DOCKER_IMAGE_NAME)
            .withNetwork(NETWORK)
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres")
            .withNetworkAliases("postgres");

    private static final DebeziumContainer DEBEZIUM_CONTAINER = new DebeziumContainer(DockerImageName.parse("debezium/connect-base:1.4"))
            .dependsOn(KAFKA_CONTAINER)
            .dependsOn(POSTGRES_CONTAINER)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams())
            .withNetwork(NETWORK);

    private String oldContainerPort;

    private boolean setupCompleted = false;

    @BeforeClass
    public static void initTestSuite() {
        final String debeziumVersion = System.getProperty("project.version");
        assumeFalse(
                "ERROR: Project version could not be identified! Make sure env var \"project.version\" is correctly passed for Maven failsafe plugin in POM.",
                null == debeziumVersion);

        DEBEZIUM_CONTAINER.withFileSystemBind(
                "target/debezium-connector-postgres-" + debeziumVersion + "-plugin/debezium-connector-postgres",
                "/kafka/connect/debezium-connector-postgres", BindMode.READ_ONLY).withKafka(KAFKA_CONTAINER);
    }

    @Before
    public void setUp() {
        assumeTrue(
                "This test suite will be skipped when the \"assembly\" profile is not active.",
                "true".equals(System.getProperty("assembly.profile.active")));
        assumeTrue(
                "This test suite will be skipped when \"assembly.descriptor\" is not set to \"connector-distribution-test\".",
                "connector-distribution-test".equals(System.getProperty("assembly.descriptor")));

        oldContainerPort = System.getProperty("database.port", "5432");
        Startables.deepStart(Stream.of(KAFKA_CONTAINER, POSTGRES_CONTAINER, DEBEZIUM_CONTAINER)).join();
        System.setProperty("database.port", String.valueOf(POSTGRES_CONTAINER.getMappedPort(5432)));

        try {
            TestHelper.dropAllSchemas();
        }
        catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
        setupCompleted = true;
    }

    @After
    public void tearDown() {
        if (setupCompleted) {
            TestHelper.dropDefaultReplicationSlot();
            TestHelper.dropPublication();
        }
        DEBEZIUM_CONTAINER.stop();
        POSTGRES_CONTAINER.stop();
        KAFKA_CONTAINER.stop();
        if (setupCompleted && null != oldContainerPort) {
            System.setProperty("database.port", oldContainerPort);
        }
    }

    @Test
    @FixFor("DBZ-2952")
    public void shouldAllowConfigUpdateWithoutChangedSlotName() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        final int recordCount = 100;

        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STMT);
        }

        Print.enable();

        ConnectorConfiguration config = DebeziumContainer.getPostgresConnectorConfiguration(POSTGRES_CONTAINER, 1)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE.name(), SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), false)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST.name(), "s1")
                .with(Heartbeat.HEARTBEAT_INTERVAL.name(), 500)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY.name(), "UPDATE s1.heartbeat SET ts=NOW();");

        final String connectorName = "my-postgres-connector";
        DEBEZIUM_CONTAINER.registerConnector(connectorName, config);

        // wait for snapshotting...
        logger.info("Waiting for snapshotting...");
        Thread.sleep(waitTimeForRecords() * 3_000L); // TODO we need to access JMX here

        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STMT);
        }
        DEBEZIUM_CONTAINER.pauseConnector(connectorName);

        config.with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST.name(), "s1,s2");

        try {
            DEBEZIUM_CONTAINER.updateOrCreateConnector(connectorName, config);
        }
        catch (IllegalStateException e) {
            if (e.getMessage()
                    .contains("Slot name \\\"debezium_1\\\" already exists and is active. Choose a unique name or stop the other process occupying the slot.")) {
                fail("Duplicate slot name error occured.");
            }
            fail(e.getMessage());
        }

        DEBEZIUM_CONTAINER.resumeConnector(connectorName);

        try {
            DEBEZIUM_CONTAINER.ensureConnectorConfigProperty(connectorName, PostgresConnectorConfig.SCHEMA_INCLUDE_LIST.name(), "s1,s2");
        }
        catch (ConditionTimeoutException e) {
            fail("Connector did not apply configuration update within configured timeout.");
        }

        try {
            DEBEZIUM_CONTAINER.ensureConnectorState(connectorName, Connector.State.RUNNING);
        }
        catch (ConditionTimeoutException e) {
            fail("Connector did not start within configured timeout.");
        }
    }

    @Test
    @FixFor("DBZ-2952")
    public void shouldNotAllowNewConnectorWithSameReplicationSlotName() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        final int recordCount = 100;

        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STMT);
        }

        Print.enable();

        ConnectorConfiguration config = DebeziumContainer.getPostgresConnectorConfiguration(POSTGRES_CONTAINER, 1)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE.name(), SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), false)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST.name(), "s1")
                .with(Heartbeat.HEARTBEAT_INTERVAL.name(), 500)
                .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY.name(), "UPDATE s1.heartbeat SET ts=NOW();");

        final String connectorName = "my-postgres-connector";
        DEBEZIUM_CONTAINER.registerConnector(connectorName, config);

        // wait for snapshotting...
        logger.info("Waiting for snapshotting...");
        Thread.sleep(waitTimeForRecords() * 3_000L); // TODO we need to access JMX here

        for (int i = 0; i < recordCount - 1; i++) {
            TestHelper.execute(INSERT_STMT);
        }
        DEBEZIUM_CONTAINER.pauseConnector(connectorName);

        config.with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST.name(), "s1,s2");

        String failingConnectorName = connectorName + "2";
        try {
            DEBEZIUM_CONTAINER.updateOrCreateConnector(failingConnectorName, config);
        }
        catch (IllegalStateException e) {
            if (!e.getMessage()
                    .contains("Slot name \\\"debezium_1\\\" already exists and is active. Choose a unique name or stop the other process occupying the slot.")) {
                fail("Expected duplicate and active slot name error did not occur.");
            }
        }

        DEBEZIUM_CONTAINER.resumeConnector(connectorName);

        boolean illegalConnectorIsRunning = true;
        try {
            DEBEZIUM_CONTAINER.ensureConnectorRegistered(failingConnectorName);
        }
        catch (ConditionTimeoutException e) {
            illegalConnectorIsRunning = false;
        }
        assertFalse("The second connector started even there's is already another connector occupying the same replication slot.", illegalConnectorIsRunning);
    }

}
