/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.mysql.signal.SetBinlogPositionSignal;
import io.debezium.util.Testing;

/**
 * Integration test for the set-binlog-position signal.
 *
 * @author Debezium Authors
 */
public class MySqlBinlogPositionSignalIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final String SERVER_NAME = "binlog_signal_test";
    private static final String SIGNAL_TABLE = "debezium_signal";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-binlog-signal.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "signal_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private MySqlTestConnection connection;

    @BeforeEach
    public void beforeEach() throws SQLException {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        Testing.Print.enable();
    }

    @AfterEach
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
        DATABASE.createAndInitialize();
        connection = (MySqlTestConnection) getTestDatabaseConnection(DATABASE.getDatabaseName());

        // Skip this test if GTID mode is enabled (binlog file/position doesn't work with GTID)
        if (isGtidModeEnabled()) {
            Testing.print("GTID mode enabled, skipping test (use shouldSkipToGtidSetViaSignal instead)");
            return;
        }

        // Start connector - use NO_DATA mode to skip initial snapshot data
        Configuration config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port", "3306"))
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NO_DATA)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("test_table"))
                .with(MySqlConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName(SIGNAL_TABLE))
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with("heartbeat.interval.ms", 1000) // Required for signal offset persistence
                .build();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot to complete (schema only with NO_DATA mode)
        waitForSnapshotToBeCompleted("mysql", SERVER_NAME);

        // Wait for streaming to be fully running
        waitForStreamingRunning("mysql", SERVER_NAME);

        // Insert some data
        connection.execute("INSERT INTO test_table VALUES (1, 'value1')");
        connection.execute("INSERT INTO test_table VALUES (2, 'value2')");
        connection.commit();

        // Give connector time to process binlog events
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        // Consume the insert events (account for heartbeat messages)
        SourceRecords records = consumeRecordsByTopic(4);
        String expectedTopic = SERVER_NAME + "." + DATABASE.getDatabaseName() + ".test_table";
        Testing.print("Expected topic: " + expectedTopic);
        Testing.print("Actual topics: " + records.topics());
        assertThat(records.recordsForTopic(expectedTopic)).hasSize(2);

        // Insert more data that we'll skip
        connection.execute("INSERT INTO test_table VALUES (3, 'value3')");
        connection.execute("INSERT INTO test_table VALUES (4, 'value4')");
        connection.commit();

        // Wait for connector to process id=3,4 and consume them to ensure
        // they are fully processed before we capture the binlog position
        waitForAvailableRecords(5, TimeUnit.SECONDS);
        consumeAvailableRecords(record -> {
        });

        // Get current binlog position AFTER the records we want to skip
        // This position will be after records 3 and 4, so when we restart the connector
        // it will skip those and start reading from record 5
        Map<String, Object> position = getCurrentBinlogPosition();
        String binlogFile = (String) position.get("file");
        Long binlogPos = (Long) position.get("position");

        // Send signal to skip to the binlog position after value3 and value4
        // This position includes all events up to and including id=3,4
        // When we restart, the connector will skip those and start from id=5
        // The signal will stop the connector after updating the offset
        String signalData = String.format(
                "{\"binlog_filename\": \"%s\", \"binlog_position\": %d, \"action\": \"stop\"}",
                binlogFile, binlogPos);

        connection.execute(
                "INSERT INTO " + SIGNAL_TABLE + " VALUES ('skip-signal-1', '" +
                        SetBinlogPositionSignal.NAME + "', '" + signalData + "')");

        // Wait for the signal to be processed and offset to be committed
        // The signal processing will trigger a heartbeat event with the new offset
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        // Consume all pending records to ensure signal is processed
        consumeAvailableRecords(record -> {
        });

        // Wait for engine shutdown (signal triggers async stop via changeEventSourceCoordinator.stop())
        // This follows the pattern from BinlogRestartIT for handling connector restarts
        waitForEngineShutdown();
        stopConnector();

        // Insert data we want to capture after the skip
        // This is done AFTER stopping to ensure id=5 is not consumed before restart
        connection.execute("INSERT INTO test_table VALUES (5, 'value5')");
        connection.commit();

        // Reinitialize test framework before starting new connector
        initializeConnectorTestFramework();

        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", SERVER_NAME);
        assertConnectorIsRunning();

        // Verify we only get record 5 (skipped 3 and 4)
        // Account for heartbeat messages during streaming
        records = consumeRecordsByTopic(2);
        List<SourceRecord> tableRecords = records.recordsForTopic(SERVER_NAME + "." + DATABASE.getDatabaseName() + ".test_table");
        assertThat(tableRecords).hasSize(1);

        Struct value = (Struct) tableRecords.get(0).value();
        Struct after = value.getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(5);
        assertThat(after.getString("value")).isEqualTo("value5");
    }

    @Test
    public void shouldSkipToGtidSetViaSignal() throws Exception {
        // Setup database
        DATABASE.createAndInitialize();
        connection = (MySqlTestConnection) getTestDatabaseConnection(DATABASE.getDatabaseName());

        // Skip this test if GTID mode is not enabled
        if (!isGtidModeEnabled()) {
            Testing.print("GTID mode not enabled, skipping test");
            return;
        }

        // Start connector - use NO_DATA mode to skip initial snapshot data
        Configuration config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port", "3306"))
                .with(MySqlConnectorConfig.SERVER_ID, 18766)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NO_DATA)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("test_table"))
                .with(MySqlConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName(SIGNAL_TABLE))
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with("heartbeat.interval.ms", 1000) // Required for signal offset persistence
                .build();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot to complete (schema only with NO_DATA mode)
        waitForSnapshotToBeCompleted("mysql", SERVER_NAME);

        // Wait for streaming to be fully running
        waitForStreamingRunning("mysql", SERVER_NAME);

        // Insert some data
        connection.execute("INSERT INTO test_table VALUES (1, 'value1')");
        connection.execute("INSERT INTO test_table VALUES (2, 'value2')");
        connection.commit();

        // Give connector time to process binlog events
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        // Consume the insert events (account for heartbeat messages)
        SourceRecords records = consumeRecordsByTopic(4);
        String expectedTopic = SERVER_NAME + "." + DATABASE.getDatabaseName() + ".test_table";
        Testing.print("Expected topic: " + expectedTopic);
        Testing.print("Actual topics: " + records.topics());
        assertThat(records.recordsForTopic(expectedTopic)).hasSize(2);

        // Insert more data that we'll skip
        connection.execute("INSERT INTO test_table VALUES (3, 'value3')");
        connection.execute("INSERT INTO test_table VALUES (4, 'value4')");
        connection.commit();

        // Wait for connector to process id=3,4 and consume them to ensure
        // they are fully processed before we capture the GTID set
        waitForAvailableRecords(5, TimeUnit.SECONDS);
        consumeAvailableRecords(record -> {
        });

        // Get current GTID set AFTER the records we want to skip
        // This captures the GTIDs for all transactions the connector has processed
        String gtidSet = getCurrentGtidSet();

        // Send signal to skip to the GTID set after value3 and value4
        // This GTID set includes all transactions up to and including id=3,4
        // When we restart, the connector will skip those and start from id=5
        // The signal will stop the connector after updating the offset
        String signalData = "{\"gtid_set\": \"" + gtidSet + "\", \"action\": \"stop\"}";

        connection.execute(
                "INSERT INTO " + SIGNAL_TABLE + " VALUES ('skip-signal-2', '" +
                        SetBinlogPositionSignal.NAME + "', '" + signalData + "')");

        // Wait for the signal to be processed and offset to be committed
        // The signal processing will trigger a heartbeat event with the new offset
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        // Consume all pending records to ensure signal is processed
        consumeAvailableRecords(record -> {
        });

        // Wait for engine shutdown (signal triggers async stop via changeEventSourceCoordinator.stop())
        // This follows the pattern from BinlogRestartIT for handling connector restarts
        waitForEngineShutdown();
        stopConnector();

        // Insert data we want to capture after the skip
        // This is done AFTER stopping to ensure id=5 is not consumed before restart
        connection.execute("INSERT INTO test_table VALUES (5, 'value5')");
        connection.commit();

        // Reinitialize test framework before starting new connector
        initializeConnectorTestFramework();

        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", SERVER_NAME);
        assertConnectorIsRunning();

        // Verify we only get record 5 (skipped 3 and 4)
        // Account for heartbeat messages during streaming
        records = consumeRecordsByTopic(2);
        List<SourceRecord> tableRecords = records.recordsForTopic(SERVER_NAME + "." + DATABASE.getDatabaseName() + ".test_table");
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
