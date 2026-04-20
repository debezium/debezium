/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.util.Testing;

/**
 * Validates that {@link BinlogConnectorConfig.SnapshotMode#NEVER} can be used when the
 * internal opt-in property {@link MySqlConnectorConfig#SNAPSHOT_MODE_NEVER_ALLOWED}
 * is explicitly set to {@code true}.
 *
 * <p>This test is {@link Disabled} by default. The {@code never} snapshot mode is not
 * supported for production use; it exists solely to preserve and document the legacy
 * "stream from the beginning of the binlog" behavior for targeted testing scenarios.
 *
 * @author Chris Cranford
 */
@Disabled("Validates internal NEVER snapshot mode behavior - not for regular CI use")
public class MySqlNeverSnapshotModeIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final String SERVER_NAME = "never_snapshot_test";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-never-snapshot.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "connector_test_ro")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @BeforeEach
    public void beforeEach() {
        stopConnector();
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
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE_NEVER_ALLOWED, true)
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
}
