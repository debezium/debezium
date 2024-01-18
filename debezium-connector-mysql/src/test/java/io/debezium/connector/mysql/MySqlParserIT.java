/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.junit.SkipTestDependingOnSslModeRule;
import io.debezium.connector.mysql.junit.SkipWhenSslModeIsNot;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.testing.testcontainers.util.ContainerImageVersions;
import io.debezium.util.Testing;

/**
 * Integration test for {@link MySqlConnector} using Testcontainers infrastructure for testing column constraints supported in MySQL 8.0.x.
 */
@SkipWhenSslModeIsNot(value = MySqlConnectorConfig.SecureConnectionMode.DISABLED, reason = "Only running with ssl disabled mode")
public class MySqlParserIT extends AbstractConnectorTest {

    @Rule
    public TestRule skipTestRule = new SkipTestDependingOnSslModeRule();

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlParserIT.class);
    private static final String DB_NAME = "inventory";

    private MySQLContainer<?> mySQLContainer;
    private Configuration config;
    private String oldContainerPort;

    @Before
    public void beforeEach() {
        String mysqlImage = ContainerImageVersions.getStableImage("quay.io/debezium/example-mysql");
        DockerImageName mysqlDockerImageName = DockerImageName.parse(mysqlImage).asCompatibleSubstituteFor("mysql");
        mySQLContainer = new MySQLContainer<>(mysqlDockerImageName)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysql")
                .withClasspathResourceMapping("/docker/conf/mysql.cnf", "/etc/mysql/conf.d/", BindMode.READ_ONLY)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withExposedPorts(3306)
                .withNetworkAliases("mysql");
        mySQLContainer.start();
        oldContainerPort = System.getProperty("database.port", "3306");

        System.setProperty("database.port", String.valueOf(mySQLContainer.getMappedPort(3306)));
        initializeConnectorTestFramework();
    }

    @After
    public void afterEach() {
        stopConnector();
        mySQLContainer.stop();
        System.setProperty("database.port", oldContainerPort);
    }

    public Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myServer1")
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT, mySQLContainer.getMappedPort(3306))
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "dbz")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, "io.debezium.relational.history.MemorySchemaHistory")
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DB_NAME)
                .with(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000);
    }

    @Test
    public void parseTableWithVisibleColumns() throws SQLException, InterruptedException {
        config = defaultConfig().build();

        Testing.Print.enable();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DB_NAME, mySQLContainer.getUsername(), mySQLContainer.getPassword())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("SELECT VERSION();");
                connection.execute("CREATE TABLE VISIBLE_COLUMN_TABLE (" +
                        "    ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "    NAME VARCHAR(100) NOT NULL," +
                        "    WORK_ID BIGINT VISIBLE" +
                        ");");
                connection.execute("INSERT INTO VISIBLE_COLUMN_TABLE VALUES (1001,'Larry',113);");
            }
        }

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(30);
        boolean statementFound = records.ddlRecordsForDatabase(DB_NAME)
                .stream()
                .anyMatch(s -> ((Struct) s.value())
                        .getString("ddl").equals("CREATE TABLE `VISIBLE_COLUMN_TABLE` (\n" +
                                "  `ID` bigint NOT NULL AUTO_INCREMENT,\n" +
                                "  `NAME` varchar(100) NOT NULL,\n" +
                                "  `WORK_ID` bigint DEFAULT NULL,\n" +
                                "  PRIMARY KEY (`ID`)\n" +
                                ") ENGINE=InnoDB AUTO_INCREMENT=1002 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"));
        assertThat(statementFound).isTrue();
    }

    @Test
    public void parseTableWithInVisibleColumns() throws SQLException, InterruptedException {
        config = defaultConfig().build();

        Testing.Print.enable();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DB_NAME, mySQLContainer.getUsername(), mySQLContainer.getPassword())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("SELECT VERSION();");
                connection.execute("CREATE TABLE INVISIBLE_COLUMN_TABLE (" +
                        " ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        " NAME VARCHAR(100) NOT NULL," +
                        " WORK_ID BIGINT INVISIBLE" +
                        ");");
                connection.execute("INSERT INTO INVISIBLE_COLUMN_TABLE VALUES (1002,'Jack');");
            }
        }

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(30);
        boolean statementFound = records.ddlRecordsForDatabase(DB_NAME)
                .stream()
                .anyMatch(s -> ((Struct) s.value())
                        .getString("ddl").equals("CREATE TABLE `INVISIBLE_COLUMN_TABLE` (\n" +
                                "  `ID` bigint NOT NULL AUTO_INCREMENT,\n" +
                                "  `NAME` varchar(100) NOT NULL,\n" +
                                "  `WORK_ID` bigint DEFAULT NULL /*!80023 INVISIBLE */,\n" +
                                "  PRIMARY KEY (`ID`)\n" +
                                ") ENGINE=InnoDB AUTO_INCREMENT=1003 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"));
        assertThat(statementFound).isTrue();
    }

    @Test
    public void parseTableCreatedWithTableStatement() throws SQLException, InterruptedException {
        config = defaultConfig().build();

        Testing.Print.enable();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DB_NAME, mySQLContainer.getUsername(), mySQLContainer.getPassword())) {
            try (JdbcConnection connection = db.connect()) {

                connection.execute("CREATE TABLE table1 (" +
                        "ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "NAME VARCHAR(100) NOT NULL" +
                        ");");
                connection.execute("CREATE TABLE table2 (" +
                        "WORK_ID BIGINT" +
                        ") TABLE table1;");
                connection.execute("INSERT INTO table2 VALUES (113, 1001,'Larry');");
            }
        }

        // Start the connector ...
        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(30);
        boolean statementFound = records.ddlRecordsForDatabase(DB_NAME)
                .stream()
                .anyMatch(s -> ((Struct) s.value())
                        .getString("ddl").equals("CREATE TABLE `table2` (\n" +
                                "  `WORK_ID` bigint DEFAULT NULL,\n" +
                                "  `ID` bigint NOT NULL DEFAULT '0',\n" +
                                "  `NAME` varchar(100) NOT NULL\n" +
                                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"));
        assertThat(statementFound).isTrue();
    }
}
