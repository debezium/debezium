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
import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * Validates that the legacy {@code never} snapshot mode can be used by combining the configuration-based
 * snapshot mode using the following settings:
 *
 * <ul>
 *     <li>snapshot schema as {@code false}</li>
 *     <li>snapshot data as {@code false}</li>
 *     <li>stream as {@code true}</li>
 * </ul>
 *
 * <p>This test serves to solely preserve and document the legacy "stream from the beginning of the binlog"
 * behavior for targeted testing scenarios. This class requires that before each test, binlogs must be
 * purged, so that any previous test state does not influence the outcome of the current test.
 *
 * @author Chris Cranford
 */
public class MySqlNeverSnapshotModeIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final String SERVER_NAME = "never_snapshot_test";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-never-snapshot.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "connector_test_ro")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @BeforeEach
    public void beforeEach() throws Exception {
        stopConnector();

        // This is required so that test state is expected
        purgeDatabaseLogs();

        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void shouldPermitNeverSnapshotModeWhenInternalPropertyEnabled() throws Exception {
        // Verifies that snapshot.mode=never is accepted (no validation error) when the
        // internal opt-in flag is set, and that the connector starts successfully and reads
        // events from the beginning of the binlog.
        final Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.CONFIGURATION_BASED)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_DATA, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_SCHEMA, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_START_STREAM, true)
                .with(BinlogConnectorConfig.USER, "replicator")
                .with(BinlogConnectorConfig.PASSWORD, "replpass")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        // With NEVER mode the connector reads from the beginning of the binlog, so it
        // should capture the INSERT statements executed by createAndInitialize().
        int expected = 9 + 9 + 4 + 5 + 1; // matches BinlogStreamingSourceIT.shouldCreateSnapshotOfSingleDatabase
        final SourceRecords records = consumeRecordsByTopic(expected);
        final int consumed = records.allRecordsInOrder().size();
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        stopConnector();
    }

    private void purgeDatabaseLogs() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection("mysql")) {
            try (JdbcConnection connection = db.connect()) {
                // make sure there's a new log file
                connection.execute("FLUSH LOGS");

                // purge all log files other than the last one
                List<String> binlogs = getBinlogs(connection);
                String lastBinlogName = binlogs.get(binlogs.size() - 1);
                connection.execute("PURGE BINARY LOGS TO '" + lastBinlogName + "'");

                // apparently, PURGE is async, as occasionally we saw the first log file still to be active
                // after that call, causing the GTID 1 (which is in the first log file) to be part of offsets
                // which is not what we expect
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
                    List<String> binlogsAfterPurge = getBinlogs(connection);

                    if (binlogsAfterPurge.size() != 1) {
                        Testing.print("Binlogs before purging: " + binlogs);
                        Testing.print("Binlogs after purging: " + binlogsAfterPurge);
                    }

                    return binlogsAfterPurge.size() == 1;
                });
            }
        }
    }

    private List<String> getBinlogs(JdbcConnection connection) throws SQLException {
        return connection.queryAndMap("SHOW BINARY LOGS", rs -> {
            List<String> binlogs = new ArrayList<>();
            while (rs.next()) {
                binlogs.add(rs.getString(1));
            }
            return binlogs;
        });
    }
}
