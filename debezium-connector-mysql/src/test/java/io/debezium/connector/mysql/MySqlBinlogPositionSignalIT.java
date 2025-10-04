/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorIT;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.mysql.signal.SetBinlogPositionSignal;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * Integration test for the set-binlog-position signal.
 *
 * @author Debezium Authors
 */
public class MySqlBinlogPositionSignalIT extends BinlogConnectorIT<MySqlConnector, MySqlPartition, MySqlOffsetContext>
        implements MySqlCommon {

    private static final String SIGNAL_TABLE = "debezium_signal";
    private static final String TEST_TABLE = "test_table";
    private static final String DATABASE_NAME = "signal_test_db";

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", DATABASE_NAME)
            .withDbHistoryPath(Testing.Files.createTestingPath("file-schema-history-signal.txt").toAbsolutePath());

    @Before
    @Override
    public void beforeEach() {
        super.beforeEach();
        Testing.Print.enable();
    }

    @After
    @Override
    public void afterEach() {
        super.afterEach();
    }

    @Test
    @FixFor("DBZ-3829")
    public void shouldProcessBinlogPositionSignal() throws Exception {
        // Initialize database
        DATABASE.createAndInitialize();
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE_NAME)) {
            conn.execute(
                    "CREATE TABLE " + TEST_TABLE + " (id INT PRIMARY KEY, value VARCHAR(100))",
                    "CREATE TABLE " + SIGNAL_TABLE + " (" +
                            "id varchar(255) PRIMARY KEY, " +
                            "type varchar(32) NOT NULL, " +
                            "data text NULL" +
                            ")");
        }

        // Configure and start connector
        Configuration config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE_NAME + "." + SIGNAL_TABLE)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        start(getConnectorClass(), config);

        // Wait for connector to start streaming
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // Insert initial data
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE_NAME)) {
            conn.execute(
                    "INSERT INTO " + TEST_TABLE + " VALUES (1, 'value1')",
                    "INSERT INTO " + TEST_TABLE + " VALUES (2, 'value2')");
        }

        // Consume initial records
        SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable(TEST_TABLE))).hasSize(2);

        // Insert more data that we'll attempt to skip
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE_NAME)) {
            conn.execute(
                    "INSERT INTO " + TEST_TABLE + " VALUES (3, 'value3')",
                    "INSERT INTO " + TEST_TABLE + " VALUES (4, 'value4')");
        }

        // Send signal to reset position (for demo purposes, using position 0)
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE_NAME)) {
            String signalData = "{\"binlog_filename\": \"mysql-bin.000001\", \"binlog_position\": 0}";
            conn.execute(
                    "INSERT INTO " + SIGNAL_TABLE + " VALUES ('test-signal-1', '" +
                            SetBinlogPositionSignal.NAME + "', '" + signalData + "')");
        }

        // Wait for connector to process signal and restart
        waitForConnectorShutdown(getConnectorName(), DATABASE.getServerName());

        // Give time for restart
        TimeUnit.SECONDS.sleep(5);

        // Verify connector is running again
        assertConnectorIsRunning();

        // After restart from position 0, we should see all records again
        records = consumeRecordsByTopic(4); // All 4 records
        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable(TEST_TABLE));
        assertThat(tableRecords).hasSize(4);
    }

    @Test
    @FixFor("DBZ-3829")
    public void shouldRejectInvalidSignalData() throws Exception {
        // Initialize database
        DATABASE.createAndInitialize();
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE_NAME)) {
            conn.execute(
                    "CREATE TABLE " + TEST_TABLE + " (id INT PRIMARY KEY, value VARCHAR(100))",
                    "CREATE TABLE " + SIGNAL_TABLE + " (" +
                            "id varchar(255) PRIMARY KEY, " +
                            "type varchar(32) NOT NULL, " +
                            "data text NULL" +
                            ")");
        }

        // Configure and start connector
        Configuration config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE_NAME + "." + SIGNAL_TABLE)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        start(getConnectorClass(), config);

        // Wait for connector to start streaming
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        // Send invalid signal (missing position)
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE_NAME)) {
            String invalidSignalData = "{\"binlog_filename\": \"mysql-bin.000001\"}";
            conn.execute(
                    "INSERT INTO " + SIGNAL_TABLE + " VALUES ('invalid-signal-1', '" +
                            SetBinlogPositionSignal.NAME + "', '" + invalidSignalData + "')");
        }

        // Give time for signal processing
        TimeUnit.SECONDS.sleep(2);

        // Connector should still be running (signal rejected but not crashed)
        assertConnectorIsRunning();
    }

    @Override
    protected MySqlPartition createPartition(String serverName, String databaseName) {
        return new MySqlPartition(serverName, databaseName);
    }

    @Override
    protected MySqlOffsetContext loadOffsets(Configuration configuration, java.util.Map<String, ?> offsets) {
        return new MySqlOffsetContext.Loader(new MySqlConnectorConfig(configuration)).load(offsets);
    }

    @Override
    protected void assertBinlogPosition(long offsetPosition, long beforeInsertsPosition) {
        assertThat(offsetPosition).isGreaterThan(beforeInsertsPosition);
    }

    @Override
    protected void assertSnapshotLockingModeIsNone(Configuration config) {
        assertThat(new MySqlConnectorConfig(config).getSnapshotLockingMode().get())
                .isEqualTo(MySqlConnectorConfig.SnapshotLockingMode.NONE);
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return MySqlConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }

    @Override
    protected Config validateConfiguration(Configuration configuration) {
        return new MySqlConnector().validate(configuration.asMap());
    }

    @Override
    protected Field getSnapshotLockingModeField() {
        return MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }
}
