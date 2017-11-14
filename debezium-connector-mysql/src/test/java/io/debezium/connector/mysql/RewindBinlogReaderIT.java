/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec
 */
public class RewindBinlogReaderIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer", "connector_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void shouldNotRewindPosition() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        validateDatabase();

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        MySQLConnection.forTestDatabase(DATABASE.getDatabaseName()).execute("INSERT INTO customers VALUES(default, 'John', 'Doe', 'john.doe@example.com')");
        // Re-start the connector ...
        start(MySqlConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1); // one additional record
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers")).size()).isEqualTo(1);
        stopConnector();
    }

    @Test
    public void shouldRewindPositionToBeginning() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.REWIND_BINLOG, "begin")
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        validateDatabase();

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        // Re-start the connector ...
        start(MySqlConnector.class, config);
        validateDatabase();
        stopConnector();
    }

    @Test
    public void shouldRewindPositionToEnd() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.REWIND_BINLOG, "end")
                              .build();

        // Start the connector ...
        start(MySqlConnector.class, config);
        validateDatabase();

        // More records may have been written (if this method were run after the others), but we don't care ...
        stopConnector();

        MySQLConnection.forTestDatabase(DATABASE.getDatabaseName()).execute("INSERT INTO customers VALUES(default, 'John', 'Doe', 'john.doe@example.com')");
        // Re-start the connector ...
        start(MySqlConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1); // one additional record
        assertThat(records.allRecordsInOrder()).isEmpty();
        stopConnector();
    }

    private void validateDatabase() throws InterruptedException {
        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(9 + 9 + 4 + 5 + 6); // 6 DDL changes
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("products_on_hand")).size()).isEqualTo(9);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("orders")).size()).isEqualTo(5);
        assertThat(records.topics().size()).isEqualTo(4 + 1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(6);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
    }

}
