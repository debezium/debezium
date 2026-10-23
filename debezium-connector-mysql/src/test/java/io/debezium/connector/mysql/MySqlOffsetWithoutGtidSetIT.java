/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

/**
 * Verifies streaming from a server with {@code gtid_mode=ON} when the offset contains a binlog position but no GTID set,
 * which is the case when the snapshot was taken before the server executed any GTID transaction, or when the offset was
 * recorded before GTIDs were enabled on the server.
 */
public class MySqlOffsetWithoutGtidSetIT extends AbstractAsyncEngineConnectorTest {

    private static final String IMAGE = System.getProperty("mysql.server.image", "container-registry.oracle.com/mysql/community-server:8.4");
    private static final String ROOT_PASSWORD = "debezium";
    private static final String SERVER_NAME = "gtid_offset_server";
    private static final String DATABASE = "gtid_offset";
    private static final String TOPIC = SERVER_NAME + "." + DATABASE + ".t";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-gtid-offset.txt").toAbsolutePath();

    private MySQLContainer container;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        stopConnector();
        if (container != null) {
            container.stop();
        }
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("debezium/dbz#2852")
    public void shouldStreamFromServerThatHasNotExecutedAnyGtidTransaction() throws Exception {
        startServer("ON");
        execute(resetBinaryLogsStatement());
        assertThat(gtidExecuted()).isEmpty();

        start(MySqlConnector.class, config(SnapshotMode.NO_DATA));
        waitForStreamingRunning("mysql", SERVER_NAME);

        execute("CREATE DATABASE " + DATABASE,
                "CREATE TABLE " + DATABASE + ".t (id INT PRIMARY KEY)",
                "INSERT INTO " + DATABASE + ".t VALUES (1)");
        assertNextRecords("c1");

        // The offset contains a GTID set now
        stopConnector();
        execute("INSERT INTO " + DATABASE + ".t VALUES (2)");
        start(MySqlConnector.class, config(SnapshotMode.NO_DATA));
        waitForStreamingRunning("mysql", SERVER_NAME);
        execute("INSERT INTO " + DATABASE + ".t VALUES (3)");
        assertNextRecords("c2", "c3");
    }

    @Test
    @FixFor("debezium/dbz#2852")
    public void shouldResumeAfterFirstGtidTransactionWhenSnapshotHasNoGtidSet() throws Exception {
        startServer("OFF");
        execute("CREATE DATABASE " + DATABASE,
                "CREATE TABLE " + DATABASE + ".t (id INT PRIMARY KEY)",
                "INSERT INTO " + DATABASE + ".t VALUES (1), (2)");
        enableGtids();
        assertThat(gtidExecuted()).isEmpty();

        start(MySqlConnector.class, config(SnapshotMode.INITIAL));
        assertNextRecords("r1", "r2");
        waitForStreamingRunning("mysql", SERVER_NAME);

        execute("INSERT INTO " + DATABASE + ".t VALUES (3)");
        assertNextRecords("c3");

        // The offset contains a GTID set now
        stopConnector();
        execute("INSERT INTO " + DATABASE + ".t VALUES (4)");
        start(MySqlConnector.class, config(SnapshotMode.INITIAL));
        waitForStreamingRunning("mysql", SERVER_NAME);
        execute("INSERT INTO " + DATABASE + ".t VALUES (5)");
        assertNextRecords("c4", "c5");
    }

    @Test
    @FixFor("debezium/dbz#2852")
    public void shouldResumeFromBinlogPositionWhenGtidsWereEnabledWhileConnectorWasStopped() throws Exception {
        startServer("OFF");
        execute("CREATE DATABASE " + DATABASE,
                "CREATE TABLE " + DATABASE + ".t (id INT PRIMARY KEY)",
                "INSERT INTO " + DATABASE + ".t VALUES (1)");

        start(MySqlConnector.class, config(SnapshotMode.INITIAL));
        assertNextRecords("r1");
        waitForStreamingRunning("mysql", SERVER_NAME);
        execute("INSERT INTO " + DATABASE + ".t VALUES (2)");
        assertNextRecords("c2");

        // The offset was recorded without GTIDs
        stopConnector();
        enableGtids();
        execute("INSERT INTO " + DATABASE + ".t VALUES (3)");
        start(MySqlConnector.class, config(SnapshotMode.INITIAL));
        waitForStreamingRunning("mysql", SERVER_NAME);
        execute("INSERT INTO " + DATABASE + ".t VALUES (4)");
        assertNextRecords("c3", "c4");

        // The offset contains a GTID set now
        stopConnector();
        execute("INSERT INTO " + DATABASE + ".t VALUES (5)");
        start(MySqlConnector.class, config(SnapshotMode.INITIAL));
        waitForStreamingRunning("mysql", SERVER_NAME);
        execute("INSERT INTO " + DATABASE + ".t VALUES (6)");
        assertNextRecords("c5", "c6");
    }

    private void startServer(String gtidMode) {
        container = new MySQLContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("mysql"))
                .withUsername("root")
                .withPassword(ROOT_PASSWORD)
                // The Oracle images only allow root to connect from localhost by default
                .withEnv("MYSQL_ROOT_HOST", "%")
                .withCommand("--gtid-mode=" + gtidMode, "--enforce-gtid-consistency=" + gtidMode)
                .withStartupTimeout(Duration.ofMinutes(3));
        container.start();
    }

    private void enableGtids() throws SQLException {
        execute("SET @@GLOBAL.enforce_gtid_consistency = ON",
                "SET @@GLOBAL.gtid_mode = OFF_PERMISSIVE",
                "SET @@GLOBAL.gtid_mode = ON_PERMISSIVE",
                "SET @@GLOBAL.gtid_mode = ON");
    }

    private String resetBinaryLogsStatement() throws SQLException {
        try (var connection = container.createConnection("");
                var statement = connection.createStatement();
                var resultSet = statement.executeQuery("SELECT VERSION()")) {
            resultSet.next();
            return resultSet.getString(1).startsWith("8.0") ? "RESET MASTER" : "RESET BINARY LOGS AND GTIDS";
        }
    }

    private String gtidExecuted() throws SQLException {
        try (var connection = container.createConnection("");
                var statement = connection.createStatement();
                var resultSet = statement.executeQuery("SELECT @@GLOBAL.gtid_executed")) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }

    private void execute(String... statements) throws SQLException {
        try (var connection = container.createConnection("");
                var statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private Configuration config(SnapshotMode snapshotMode) {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, container.getHost())
                .with(MySqlConnectorConfig.PORT, container.getMappedPort(3306))
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, ROOT_PASSWORD)
                .with(MySqlConnectorConfig.SERVER_ID, 18766)
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.MySqlSecureConnectionMode.DISABLED)
                .with("driver.allowPublicKeyRetrieval", "true")
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .build();
    }

    /**
     * Asserts the operation and id of the next records, so that a transaction that is read again fails the assertion.
     */
    private void assertNextRecords(String... operationsAndIds) throws InterruptedException {
        final List<SourceRecord> records = consumeRecordsByTopic(operationsAndIds.length).recordsForTopic(TOPIC);
        assertThat(records).extracting(this::operationAndId).containsExactly(operationsAndIds);
    }

    private String operationAndId(SourceRecord record) {
        final Struct value = (Struct) record.value();
        return value.getString(Envelope.FieldName.OPERATION) + value.getStruct(Envelope.FieldName.AFTER).getInt32("id");
    }
}
