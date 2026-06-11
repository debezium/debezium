/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.tablemapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Comparator;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * Integration test for RocksDb-based table mapping storage with MySQL connector.
 * Tests that RocksDb storage is properly used, cleared, and rebuilt across connector restarts.
 *
 * @author Debezium Authors
 */
public class RocksDbTableMappingStorageIT extends AbstractAsyncEngineConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-rocksdb-it.txt").toAbsolutePath();
    private static final Path ROCKSDB_TABLES_PATH = Testing.Files.createTestingPath("rocksdb-storage-it-tables").toAbsolutePath();

    private static final String SERVER_NAME = "rocksdb_it_test";
    private static final String DATABASE_NAME = "rocksdb_storage_it_test";
    private static final String PRIVILEGED_USER = "mysqluser";
    private static final String PRIVILEGED_PASSWORD = "mysqlpw";
    private static final String ROOT_PASSWORD = PRIVILEGED_PASSWORD;
    private static final DockerImageName IMAGE = DockerImageName.parse("quay.io/debezium/example-mysql")
            .asCompatibleSubstituteFor("mysql");

    private static final MySQLContainer container = new MySQLContainer(IMAGE)
            .withUsername(PRIVILEGED_USER)
            .withPassword(PRIVILEGED_PASSWORD)
            .withEnv("MYSQL_ROOT_HOST", "%")
            .withStartupTimeout(Duration.ofSeconds(180));

    private JdbcConnection connection;

    @BeforeAll
    public static void startDatabase() {
        container.start();
    }

    @AfterAll
    public static void stopDatabase() {
        container.stop();
    }

    @BeforeEach
    public void beforeEach() throws SQLException, IOException {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        deleteDirectory(ROCKSDB_TABLES_PATH);

        // Create test database using root
        try (JdbcConnection conn = adminConnection()) {
            conn.execute(
                    "DROP DATABASE IF EXISTS " + DATABASE_NAME,
                    "CREATE DATABASE " + DATABASE_NAME,
                    "GRANT ALL PRIVILEGES ON " + DATABASE_NAME + ".* TO '" + PRIVILEGED_USER + "'@'%'",
                    "GRANT RELOAD, FLUSH_TABLES, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '" + PRIVILEGED_USER + "'@'%'");
        }
    }

    private JdbcConnection adminConnection() throws SQLException {
        final JdbcConfiguration jdbcConfig = JdbcConfiguration.create()
                .withHostname(container.getHost())
                .withPort(container.getMappedPort(MySQLContainer.MYSQL_PORT))
                .withUser("root")
                .withPassword(ROOT_PASSWORD)
                .build();
        final String url = "jdbc:mysql://${hostname}:${port}";
        return new JdbcConnection(jdbcConfig, JdbcConnection.patternBasedFactory(url), "`", "`");
    }

    private JdbcConnection testConnection() throws SQLException {
        final JdbcConfiguration jdbcConfig = JdbcConfiguration.create()
                .withHostname(container.getHost())
                .withPort(container.getMappedPort(MySQLContainer.MYSQL_PORT))
                .withUser(PRIVILEGED_USER)
                .withPassword(PRIVILEGED_PASSWORD)
                .withDatabase(DATABASE_NAME)
                .build();
        final String url = "jdbc:mysql://${hostname}:${port}/${dbname}";
        return new JdbcConnection(jdbcConfig, JdbcConnection.patternBasedFactory(url), "`", "`");
    }

    @AfterEach
    public void afterEach() throws SQLException, IOException {
        try {
            stopConnector();
        }
        finally {
            try (JdbcConnection conn = adminConnection()) {
                conn.execute("DROP DATABASE IF EXISTS " + DATABASE_NAME);
            }
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
            deleteDirectory(ROCKSDB_TABLES_PATH);
        }
    }

    @Test
    public void shouldUseRocksDBStorageAndRebuildAfterRestart() throws Exception {
        // Create four tables to test with
        try (JdbcConnection conn = testConnection()) {
            conn.execute(
                    "USE " + DATABASE_NAME,
                    "CREATE TABLE table1 (id INT PRIMARY KEY, name VARCHAR(50))",
                    "CREATE TABLE table2 (id INT PRIMARY KEY, value INT)",
                    "CREATE TABLE table3 (id INT PRIMARY KEY, description TEXT)",
                    "CREATE TABLE table4 (id INT PRIMARY KEY, status VARCHAR(20))",
                    "INSERT INTO table1 VALUES (1, 'Alice')",
                    "INSERT INTO table2 VALUES (1, 100)",
                    "INSERT INTO table3 VALUES (1, 'First record')",
                    "INSERT INTO table4 VALUES (1, 'active')");
        }

        // Configure connector with RocksDb storage and cache size of 1
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, container.getHost())
                .with(MySqlConnectorConfig.PORT, container.getMappedPort(MySQLContainer.MYSQL_PORT))
                .with(MySqlConnectorConfig.USER, PRIVILEGED_USER)
                .with(MySqlConnectorConfig.PASSWORD, PRIVILEGED_PASSWORD)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, io.debezium.storage.file.history.FileSchemaHistory.class)
                .with(io.debezium.storage.file.history.FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH.toString())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                // Configure RocksDb storage for tables; schemas remain cache-only
                .with("memory.management.schemas.class", RocksDbTableMappingStorage.class.getName())
                .with("memory.management.tables.class", RocksDbTableMappingStorage.class.getName())
                // Set cache size to 1 to force RocksDb usage
                .with("memory.management.cache.size", 1)
                // Configure RocksDb paths (type-specific)
                .with("memory.management.tables.rocksdb.path", ROCKSDB_TABLES_PATH.toString())
                .with("memory.management.tables.rocksdb.cleanup", false)
                .build();

        // Start connector and wait for snapshot
        start(MySqlConnector.class, config);

        // Consume initial snapshot records (4 records from 4 tables)
        waitForSnapshotToBeCompleted("mysql", SERVER_NAME);
        final SourceRecords snapshotRecords = consumeRecordsByTopic(4, 10);

        assertThat(snapshotRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1")).hasSize(1);
        assertThat(snapshotRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table2")).hasSize(1);
        assertThat(snapshotRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table3")).hasSize(1);
        assertThat(snapshotRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table4")).hasSize(1);

        // Verify RocksDb directory was created for tables only
        assertThat(ROCKSDB_TABLES_PATH.toFile().exists()).isTrue();
        assertThat(ROCKSDB_TABLES_PATH.toFile().isDirectory()).isTrue();
        assertThat(ROCKSDB_TABLES_PATH.toFile().listFiles()).isNotEmpty();

        // Insert streaming records
        try (JdbcConnection conn = testConnection()) {
            conn.execute(
                    "USE " + DATABASE_NAME,
                    "INSERT INTO table1 VALUES (2, 'Bob')",
                    "INSERT INTO table2 VALUES (2, 200)",
                    "INSERT INTO table3 VALUES (2, 'Second record')",
                    "INSERT INTO table4 VALUES (2, 'inactive')");
        }

        // Consume streaming records
        final SourceRecords streamingRecords = consumeRecordsByTopic(4, 10);
        assertThat(streamingRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1")).hasSize(1);
        assertThat(streamingRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table2")).hasSize(1);
        assertThat(streamingRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table3")).hasSize(1);
        assertThat(streamingRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table4")).hasSize(1);

        // Verify data from streaming records
        final SourceRecord table1Record = streamingRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1").get(0);
        final Struct table1Value = (Struct) table1Record.value();
        assertThat(table1Value.getStruct("after").getInt32("id")).isEqualTo(2);
        assertThat(table1Value.getStruct("after").getString("name")).isEqualTo("Bob");

        // Stop connector
        stopConnector();
        waitForConnectorShutdown(SERVER_NAME, DATABASE_NAME);

        // Verify RocksDb files still exist (cleanup is false)
        assertThat(ROCKSDB_TABLES_PATH.toFile().exists()).isTrue();

        // Insert more records while connector is stopped
        try (JdbcConnection conn = testConnection()) {
            conn.execute(
                    "USE " + DATABASE_NAME,
                    "INSERT INTO table1 VALUES (3, 'Charlie')",
                    "INSERT INTO table2 VALUES (3, 300)",
                    "INSERT INTO table3 VALUES (3, 'Third record')",
                    "INSERT INTO table4 VALUES (3, 'pending')");
        }

        // Restart connector - storage should be cleared and rebuilt
        start(MySqlConnector.class, config);

        // Wait for connector to start streaming
        waitForStreamingRunning("mysql", SERVER_NAME);

        // Consume records inserted while connector was stopped
        final SourceRecords restartRecords = consumeRecordsByTopic(4, 10);
        assertThat(restartRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1")).hasSize(1);
        assertThat(restartRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table2")).hasSize(1);
        assertThat(restartRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table3")).hasSize(1);
        assertThat(restartRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table4")).hasSize(1);

        // Verify data from restart records
        final SourceRecord table1RestartRecord = restartRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1").get(0);
        final Struct table1RestartValue = (Struct) table1RestartRecord.value();
        assertThat(table1RestartValue.getStruct("after").getInt32("id")).isEqualTo(3);
        assertThat(table1RestartValue.getStruct("after").getString("name")).isEqualTo("Charlie");

        // Insert final records to verify storage is working after restart
        try (JdbcConnection conn = testConnection()) {
            conn.execute(
                    "USE " + DATABASE_NAME,
                    "INSERT INTO table1 VALUES (4, 'David')",
                    "INSERT INTO table2 VALUES (4, 400)",
                    "INSERT INTO table3 VALUES (4, 'Fourth record')",
                    "INSERT INTO table4 VALUES (4, 'completed')");
        }

        // Consume final records
        final SourceRecords finalRecords = consumeRecordsByTopic(4, 10);
        assertThat(finalRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1")).hasSize(1);
        assertThat(finalRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table2")).hasSize(1);
        assertThat(finalRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table3")).hasSize(1);
        assertThat(finalRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table4")).hasSize(1);

        // Verify final data
        final SourceRecord table1FinalRecord = finalRecords.recordsForTopic(SERVER_NAME + "." + DATABASE_NAME + ".table1").get(0);
        final Struct table1FinalValue = (Struct) table1FinalRecord.value();
        assertThat(table1FinalValue.getStruct("after").getInt32("id")).isEqualTo(4);
        assertThat(table1FinalValue.getStruct("after").getString("name")).isEqualTo("David");

        stopConnector();
    }

    @Test
    public void shouldCleanupRocksDBStorageByDefault() throws Exception {
        // Create test tables
        try (JdbcConnection conn = testConnection()) {
            conn.execute(
                    "USE " + DATABASE_NAME,
                    "CREATE TABLE cleanup_table1 (id INT PRIMARY KEY, data VARCHAR(50))",
                    "CREATE TABLE cleanup_table2 (id INT PRIMARY KEY, value INT)",
                    "INSERT INTO cleanup_table1 VALUES (1, 'test')",
                    "INSERT INTO cleanup_table2 VALUES (1, 999)");
        }

        // Configure connector WITHOUT specifying cleanup config (should default to true)
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, container.getHost())
                .with(MySqlConnectorConfig.PORT, container.getMappedPort(MySQLContainer.MYSQL_PORT))
                .with(MySqlConnectorConfig.USER, PRIVILEGED_USER)
                .with(MySqlConnectorConfig.PASSWORD, PRIVILEGED_PASSWORD)
                .with(MySqlConnectorConfig.SERVER_ID, 18766)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, SERVER_NAME + "_cleanup")
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, io.debezium.storage.file.history.FileSchemaHistory.class)
                .with(io.debezium.storage.file.history.FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH.toString())
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with("memory.management.schemas.class", RocksDbTableMappingStorage.class.getName())
                .with("memory.management.tables.class", RocksDbTableMappingStorage.class.getName())
                .with("memory.management.cache.size", 1)
                .with("memory.management.tables.rocksdb.path", ROCKSDB_TABLES_PATH.toString())
                // NOTE: NOT specifying memory.management.*.rocksdb.cleanup - should default to true
                .build();

        start(MySqlConnector.class, config);

        waitForSnapshotToBeCompleted("mysql", SERVER_NAME + "_cleanup");
        consumeRecordsByTopic(2, 10);

        // Verify RocksDb directory exists for tables only while connector is running
        assertThat(ROCKSDB_TABLES_PATH.toFile().exists()).isTrue();

        // Stop connector
        stopConnector();
        waitForConnectorShutdown(SERVER_NAME + "_cleanup", DATABASE_NAME);

        // Verify RocksDb directory was cleaned up by default
        assertThat(ROCKSDB_TABLES_PATH.toFile().exists()).isFalse();
    }

    private void deleteDirectory(final Path directory) throws IOException {
        if (java.nio.file.Files.exists(directory)) {
            java.nio.file.Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            java.nio.file.Files.delete(path);
                        }
                        catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }
}
