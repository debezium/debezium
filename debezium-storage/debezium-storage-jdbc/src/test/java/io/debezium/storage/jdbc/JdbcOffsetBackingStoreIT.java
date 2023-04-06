/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.OFFSET_STORAGE_JDBC_PASSWORD;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.OFFSET_STORAGE_JDBC_URI;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.OFFSET_STORAGE_JDBC_USER;
import static io.debezium.storage.jdbc.JdbcOffsetBackingStore.OFFSET_STORAGE_TABLE_NAME;
import static io.debezium.storage.jdbc.history.JdbcSchemaHistory.JDBC_PASSWORD;
import static io.debezium.storage.jdbc.history.JdbcSchemaHistory.JDBC_URI;
import static io.debezium.storage.jdbc.history.JdbcSchemaHistory.JDBC_USER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.storage.jdbc.history.JdbcSchemaHistory;
import org.junit.*;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.UniqueDatabase;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * @author Kanthi Subramanian
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class JdbcOffsetBackingStoreIT extends AbstractConnectorTest {
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = new UniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("schema-history.db").toAbsolutePath();

    private static final String USER = "debezium";
    private static final String PASSWORD = "dbz";
    private static final String PRIVILEGED_USER = "mysqluser";
    private static final String PRIVILEGED_PASSWORD = "mysqlpassword";
    private static final String ROOT_PASSWORD = "debezium";
    private static final String DBNAME = "inventory";
    private static final String IMAGE = "debezium/example-mysql";
    private static final Integer PORT = 3306;
    private static final String TOPIC_PREFIX = "test";
    private static final String TABLE_NAME = "schematest";

    private static final GenericContainer<?> container = new GenericContainer<>(IMAGE)
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
            .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .withEnv("MYSQL_USER", PRIVILEGED_USER)
            .withEnv("MYSQL_PASSWORD", PRIVILEGED_PASSWORD)
            .withExposedPorts(PORT)
            .withStartupTimeout(Duration.ofSeconds(180));

    @BeforeClass
    public static void startDatabase() {
        container.start();
    }

    @AfterClass
    public static void stopDatabase() {
        container.stop();
    }

    @Before
    public void beforeEach() throws SQLException {
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);

        try (MySqlTestConnection conn = testConnection()) {
            conn.execute(
                    "DROP TABLE IF EXISTS schematest",
                    "CREATE TABLE schematest (id INT PRIMARY KEY, val VARCHAR(16))",
                    "INSERT INTO schematest VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
        }

        stopConnector();
    }

    @After
    public void afterEach() throws SQLException {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }

        try (MySqlTestConnection conn = testConnection()) {
            conn.execute("DROP TABLE IF EXISTS schematest");
        }
    }

    private String topicName() {
        return String.format("%s.%s.%s", TOPIC_PREFIX, DBNAME, TABLE_NAME);
    }

    protected Configuration.Builder schemaHistory(Configuration.Builder builder) {
        return builder
                .with(JDBC_URI, "jdbc:sqlite:" + SCHEMA_HISTORY_PATH)
                .with(JDBC_USER, "user")
                .with(JDBC_PASSWORD, "pass");
    }

    private Configuration.Builder config(String jdbcUri) {

        final Configuration.Builder builder = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, container.getHost())
                .with(MySqlConnectorConfig.PORT, container.getMappedPort(PORT))
                .with(MySqlConnectorConfig.USER, USER)
                .with(MySqlConnectorConfig.PASSWORD, PASSWORD)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DBNAME)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DBNAME + "." + TABLE_NAME)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, JdbcSchemaHistory.class)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(OFFSET_STORAGE_JDBC_URI.name(), jdbcUri)
                .with(OFFSET_STORAGE_JDBC_USER.name(), "user")
                .with(OFFSET_STORAGE_JDBC_PASSWORD.name(), "pass")
                .with(OFFSET_STORAGE_TABLE_NAME.name(), "offsets_jdbc")
                .with("offset.flush.interval.ms", "1000")
                .with("offset.storage", "io.debezium.storage.jdbc.JdbcOffsetBackingStore");

        return schemaHistory(builder);
    }

    private MySqlTestConnection testConnection() {
        final JdbcConfiguration jdbcConfig = JdbcConfiguration.create()
                .withHostname(container.getHost())
                .withPort(container.getMappedPort(PORT))
                .withUser(PRIVILEGED_USER)
                .withPassword(PRIVILEGED_PASSWORD)
                .withDatabase(DBNAME)
                .build();
        return new MySqlTestConnection(jdbcConfig);
    }

    @Test
    public void shouldStartCorrectlyWithJDBCOffsetStorage() throws SQLException, InterruptedException, IOException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        File dbFile = File.createTempFile("test-", "db");
        String jdbcUri = String.format("jdbc:sqlite:%s", dbFile.getAbsolutePath());

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        Configuration config = config(jdbcUri).build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", TOPIC_PREFIX);

        consumeRecordsByTopic(4);
        validateIfDataIsCreatedInJDBCDatabase(jdbcUri, "user", "pass", "offsets_jdbc");
    }

    /**
     * Function to validate the offset storage data that is created
     * in Database.
     *
     * @param jdbcUri
     * @param jdbcUser
     * @param jdbcPassword
     */
    private void validateIfDataIsCreatedInJDBCDatabase(String jdbcUri, String jdbcUser,
                                                       String jdbcPassword, String jdbcTableName) {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection(jdbcUri, jdbcUser, jdbcPassword);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30); // set timeout to 30 sec.

            ResultSet rs = statement.executeQuery(String.format("select * from %s", jdbcTableName));
            while (rs.next()) {
                String offsetKey = rs.getString("offset_key");
                String offsetValue = rs.getString("offset_val");
                String recordInsertTimestamp = rs.getString("record_insert_ts");
                String recordInsertSequence = rs.getString("record_insert_seq");

                Assert.assertFalse(offsetKey.isBlank() && offsetKey.isEmpty());
                Assert.assertFalse(offsetValue.isBlank() && offsetValue.isEmpty());
                Assert.assertFalse(recordInsertTimestamp.isBlank() && recordInsertTimestamp.isEmpty());
                Assert.assertFalse(recordInsertSequence.isBlank() && recordInsertSequence.isEmpty());

            }

        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
