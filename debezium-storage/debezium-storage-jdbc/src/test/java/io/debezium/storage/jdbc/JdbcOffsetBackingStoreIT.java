/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistory;
import io.debezium.storage.jdbc.history.JdbcSchemaHistoryConfig;
import io.debezium.storage.jdbc.offset.JdbcOffsetBackingStoreConfig;
import io.debezium.util.Testing;

/**
 * @author Kanthi Subramanian
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class JdbcOffsetBackingStoreIT extends AbstractConnectorTest {
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

        try (JdbcConnection conn = testConnection()) {
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

        try (JdbcConnection conn = testConnection()) {
            conn.execute("DROP TABLE IF EXISTS schematest");
        }
    }

    protected Configuration.Builder schemaHistory(Configuration.Builder builder) {
        return builder
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + SCHEMA_HISTORY_PATH)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass");
    }

    private Configuration.Builder config(String jdbcUrl) {

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
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_JDBC_URL.name(), jdbcUrl)
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_USER.name(), "user")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_PASSWORD.name(), "pass")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_NAME.name(), "offsets_jdbc")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_DDL.name(),
                        "CREATE TABLE %s(id VARCHAR(36) NOT NULL, " +
                                "offset_key VARCHAR(1255), offset_val VARCHAR(1255)," +
                                "record_insert_ts TIMESTAMP NOT NULL," +
                                "record_insert_seq INTEGER NOT NULL" +
                                ")")
                .with(JdbcOffsetBackingStoreConfig.OFFSET_STORAGE_PREFIX + JdbcOffsetBackingStoreConfig.PROP_TABLE_SELECT.name(),
                        "SELECT id, offset_key, offset_val FROM %s " +
                                "ORDER BY record_insert_ts, record_insert_seq")
                .with("offset.flush.interval.ms", "1000")
                .with("offset.storage", "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");

        return schemaHistory(builder);
    }

    private JdbcConnection testConnection() {
        final JdbcConfiguration jdbcConfig = JdbcConfiguration.create()
                .withHostname(container.getHost())
                .withPort(container.getMappedPort(PORT))
                .withUser(PRIVILEGED_USER)
                .withPassword(PRIVILEGED_PASSWORD)
                .withDatabase(DBNAME)
                .build();
        final String url = "jdbc:mysql://${hostname}:${port}/${dbname}";
        return new JdbcConnection(jdbcConfig, JdbcConnection.patternBasedFactory(url), "`", "`");
    }

    @Test
    public void shouldStartCorrectlyWithJdbcOffsetStorage() throws InterruptedException, IOException {
        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        File dbFile = File.createTempFile("test-", "db");
        String jdbcUrl = String.format("jdbc:sqlite:%s", dbFile.getAbsolutePath());

        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        Configuration config = config(jdbcUrl).build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", TOPIC_PREFIX);

        consumeRecordsByTopic(4);
        validateIfDataIsCreatedInJDBCDatabase(jdbcUrl, "user", "pass", "offsets_jdbc");
    }

    /**
     * Function to validate the offset storage data that is created
     * in Database.
     *
     * @param jdbcUrl
     * @param jdbcUser
     * @param jdbcPassword
     */
    private void validateIfDataIsCreatedInJDBCDatabase(String jdbcUrl, String jdbcUser,
                                                       String jdbcPassword, String jdbcTableName) {
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
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
