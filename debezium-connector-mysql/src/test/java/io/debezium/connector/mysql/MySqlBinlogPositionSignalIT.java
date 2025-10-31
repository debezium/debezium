/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.signal.SetBinlogPositionSignal;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test for the set-binlog-position signal.
 *
 * @author Debezium Authors
 */
public class MySqlBinlogPositionSignalIT extends AbstractAsyncEngineConnectorTest {

    private static final String SERVER_NAME = "binlog_signal_test";
    private static final String SIGNAL_TABLE = "debezium_signal";
    private MySqlTestConnection connection;

    @Before
    public void beforeEach() throws SQLException {
        stopConnector();
        connection = MySqlTestConnection.forTestDatabase("mysql", Map.of("protocol", "jdbc:mysql"));
        connection.connect();

        initializeConnectorTestFramework();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Print.enable();
    }

    @After
    public void afterEach() throws SQLException {
        try {
            stopConnector();
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void shouldSkipToBinlogPositionViaSignal() throws Exception {
        // Setup database
        connection.execute(
                "DROP DATABASE IF EXISTS signal_test",
                "CREATE DATABASE signal_test",
                "USE signal_test",
                "CREATE TABLE test_table (id INT PRIMARY KEY, value VARCHAR(100))",
                "CREATE TABLE " + SIGNAL_TABLE + " (" +
                        "id varchar(255) PRIMARY KEY, " +
                        "type varchar(32) NOT NULL, " +
                        "data text NULL" +
                        ")");

        // Start connector
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "signal_test")
                .with(MySqlConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, "signal_test." + SIGNAL_TABLE)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", Testing.Files.createTestingPath("schema-history.txt").toString())
                .with("database.connectionTimeZone", "US/Samoa")
                .build();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot to complete
        waitForSnapshotToBeCompleted("mysql", SERVER_NAME);

        // Insert some data
        connection.execute(
                "INSERT INTO test_table VALUES (1, 'value1')",
                "INSERT INTO test_table VALUES (2, 'value2')");

        // Consume the insert events
        SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(SERVER_NAME + ".signal_test.test_table")).hasSize(2);

        // Insert more data that we'll skip
        connection.execute(
                "INSERT INTO test_table VALUES (3, 'value3')",
                "INSERT INTO test_table VALUES (4, 'value4')");

        // Get current binlog position AFTER the records we want to skip
        // This position will be after records 3 and 4, so when we restart the connector
        // it will skip those and start reading from record 5
        Map<String, Object> position = getCurrentBinlogPosition();
        String binlogFile = (String) position.get("file");
        Long binlogPos = (Long) position.get("position");

        // Insert data we want to capture after the skip
        connection.execute("INSERT INTO test_table VALUES (5, 'value5')");

        // Send signal to skip to the position before value3 and value4
        String signalData = String.format(
                "{\"binlog_filename\": \"%s\", \"binlog_position\": %d}",
                binlogFile, binlogPos);

        connection.execute(
                "INSERT INTO " + SIGNAL_TABLE + " VALUES ('skip-signal-1', '" +
                        SetBinlogPositionSignal.NAME + "', '" + signalData + "')");

        // Wait for the signal to be processed and offset to be committed
        // The signal processing will trigger a heartbeat event with the new offset
        waitForAvailableRecords(5, java.util.concurrent.TimeUnit.SECONDS);

        // Consume all pending records to ensure signal is processed
        consumeAvailableRecords(record -> {});

        // Stop and restart the connector to apply the new binlog position
        stopConnector();
        start(MySqlConnector.class, config);

        // Verify we only get record 5 (skipped 3 and 4)
        records = consumeRecordsByTopic(1);
        List<SourceRecord> tableRecords = records.recordsForTopic(SERVER_NAME + ".signal_test.test_table");
        assertThat(tableRecords).hasSize(1);

        Struct value = (Struct) tableRecords.get(0).value();
        Struct after = value.getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(5);
        assertThat(after.getString("value")).isEqualTo("value5");
    }

    @Test
    public void shouldSkipToGtidSetViaSignal() throws Exception {
        // Skip this test if GTID mode is not enabled
        if (!isGtidModeEnabled()) {
            Testing.print("GTID mode not enabled, skipping test");
            return;
        }

        // Setup database
        connection.execute(
                "DROP DATABASE IF EXISTS signal_test",
                "CREATE DATABASE signal_test",
                "USE signal_test",
                "CREATE TABLE test_table (id INT PRIMARY KEY, value VARCHAR(100))",
                "CREATE TABLE " + SIGNAL_TABLE + " (" +
                        "id varchar(255) PRIMARY KEY, " +
                        "type varchar(32) NOT NULL, " +
                        "data text NULL" +
                        ")");

        // Start connector
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18766)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "signal_test")
                .with(MySqlConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, "signal_test." + SIGNAL_TABLE)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", Testing.Files.createTestingPath("schema-history-gtid.txt").toString())
                .with("database.connectionTimeZone", "US/Samoa")
                .build();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot to complete
        waitForSnapshotToBeCompleted("mysql", SERVER_NAME);

        // Insert some data
        connection.execute(
                "INSERT INTO test_table VALUES (1, 'value1')",
                "INSERT INTO test_table VALUES (2, 'value2')");

        // Consume the insert events
        SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(SERVER_NAME + ".signal_test.test_table")).hasSize(2);

        // Insert more data that we'll skip
        connection.execute(
                "INSERT INTO test_table VALUES (3, 'value3')",
                "INSERT INTO test_table VALUES (4, 'value4')");

        // Get current GTID set AFTER the records we want to skip
        String gtidSet = getCurrentGtidSet();

        // Insert data we want to capture after the skip
        connection.execute("INSERT INTO test_table VALUES (5, 'value5')");

        // Send signal to skip to the GTID set before value3 and value4
        String signalData = "{\"gtid_set\": \"" + gtidSet + "\"}";

        connection.execute(
                "INSERT INTO " + SIGNAL_TABLE + " VALUES ('skip-signal-2', '" +
                        SetBinlogPositionSignal.NAME + "', '" + signalData + "')");

        // Wait for the signal to be processed and offset to be committed
        // The signal processing will trigger a heartbeat event with the new offset
        waitForAvailableRecords(5, java.util.concurrent.TimeUnit.SECONDS);

        // Consume all pending records to ensure signal is processed
        consumeAvailableRecords(record -> {});

        // Stop and restart the connector to apply the new GTID position
        stopConnector();
        start(MySqlConnector.class, config);

        // Verify we only get record 5 (skipped 3 and 4)
        records = consumeRecordsByTopic(1);
        List<SourceRecord> tableRecords = records.recordsForTopic(SERVER_NAME + ".signal_test.test_table");
        assertThat(tableRecords).hasSize(1);

        Struct value = (Struct) tableRecords.get(0).value();
        Struct after = value.getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(5);
        assertThat(after.getString("value")).isEqualTo("value5");
    }

    private Map<String, Object> getCurrentBinlogPosition() throws SQLException {
        // Try SHOW BINARY LOG STATUS (MySQL 8.0.22+) first, fall back to SHOW MASTER STATUS
        try {
            return connection.queryAndMap("SHOW BINARY LOG STATUS", rs -> {
                if (rs.next()) {
                    return Map.of(
                            "file", rs.getString(1),
                            "position", rs.getLong(2));
                }
                throw new IllegalStateException("Could not get binlog position");
            });
        }
        catch (SQLException e) {
            // Fall back to legacy command for older MySQL versions
            return connection.queryAndMap("SHOW MASTER STATUS", rs -> {
                if (rs.next()) {
                    return Map.of(
                            "file", rs.getString(1),
                            "position", rs.getLong(2));
                }
                throw new IllegalStateException("Could not get binlog position");
            });
        }
    }

    private String getCurrentGtidSet() throws SQLException {
        return connection.queryAndMap("SELECT @@GLOBAL.gtid_executed", rs -> {
            if (rs.next()) {
                return rs.getString(1);
            }
            throw new IllegalStateException("Could not get GTID set");
        });
    }

    private boolean isGtidModeEnabled() {
        try {
            return connection.queryAndMap("SELECT @@GLOBAL.gtid_mode",
                    rs -> rs.next() && "ON".equals(rs.getString(1)));
        }
        catch (SQLException e) {
            return false;
        }
    }
}
