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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.UniqueDatabase;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

/**
 * @author Kanthi Subramanian
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class JdbcOffsetBackingStoreIT extends AbstractConnectorTest {
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = new UniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        // stopConnector();
        // DATABASE.createAndInitialize();
        // RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        // Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            // Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
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
        config = Configuration.create()

                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(CommonConnectorConfig.TOPIC_PREFIX, DATABASE.getServerName())
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "connector_test")
                .with("database.whitelist", "connector_test")

                .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .with(OFFSET_STORAGE_JDBC_URI.name(), jdbcUri)
                .with(OFFSET_STORAGE_JDBC_USER.name(), "user")
                .with(OFFSET_STORAGE_JDBC_PASSWORD.name(), "pass")
                .with(OFFSET_STORAGE_TABLE_NAME.name(), "offsets_jdbc")
                .with("snapshot.mode", "initial")
                .with("offset.flush.interval.ms", "1000")
                .with("offset.storage", "io.debezium.storage.jdbc.JdbcOffsetBackingStore")

                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Thread.sleep(4000);
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
