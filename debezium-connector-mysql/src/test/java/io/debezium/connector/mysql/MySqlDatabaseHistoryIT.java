/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
*
* The test to verify whether DDL is stored correctly in database history.
*
* @author Jiri Pechanec
*/
public class MySqlDatabaseHistoryIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-json.txt")
            .toAbsolutePath();

    private static final int TABLE_COUNT = 2;

    private UniqueDatabase DATABASE;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE = new UniqueDatabase("history", "history-dbz")
                .withDbHistoryPath(DB_HISTORY_PATH);
        DATABASE.createAndInitialize();

        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-3485")
    public void shouldUseQuotedNameInDrop() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        assertDdls(records);
        stopConnector();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();
        stopConnector();
    }

    @Test
    @FixFor("DBZ-3399")
    public void shouldStoreSingleRename() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        assertDdls(records);
        try (MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            connection.execute("RENAME TABLE `t-1` TO `new-t-1`");
        }
        records = consumeRecordsByTopic(1);
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        Assertions.assertThat(getDdl(schemaChanges, 0)).startsWith("RENAME TABLE `t-1` TO `new-t-1`");

        stopConnector();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();
        stopConnector();
    }

    @Test
    @FixFor("DBZ-3399")
    public void shouldStoreMultipleRenames() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnector.IMPLEMENTATION_PROP, "new")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        try (MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            connection.execute("RENAME TABLE `t-1` TO `new-t-1`, `t.2` TO `new.t.2`");
        }
        records = consumeRecordsByTopic(2);
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        Assertions.assertThat(getDdl(schemaChanges, 0)).startsWith("RENAME TABLE `t-1` TO `new-t-1`");
        Assertions.assertThat(getDdl(schemaChanges, 1)).startsWith("RENAME TABLE `t.2` TO `new.t.2`");

        stopConnector();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();
        stopConnector();
    }

    @Test
    @FixFor("DBZ-3399")
    public void shouldStoreAlterRename() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();
        // SET + USE + Drop DB + create DB + CREATE/DROP for each table
        SourceRecords records = consumeRecordsByTopic(1 + 1 + 1 + 1 + TABLE_COUNT * 2);
        try (MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            connection.execute("ALTER TABLE `t-1` RENAME TO `new-t-1`");
        }
        records = consumeRecordsByTopic(1);
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        Assertions.assertThat(getDdl(schemaChanges, 0)).startsWith("ALTER TABLE `t-1` RENAME TO `new-t-1`");

        stopConnector();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();
        stopConnector();
    }

    private void assertDdls(SourceRecords records) {
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        int index = 0;
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("SET");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("DROP TABLE IF EXISTS `" + DATABASE.getDatabaseName() + "`.`t-1`");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("DROP TABLE IF EXISTS `" + DATABASE.getDatabaseName() + "`.`t.2`");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("DROP DATABASE IF EXISTS `" + DATABASE.getDatabaseName() + "`");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE DATABASE `" + DATABASE.getDatabaseName() + "`");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("USE `" + DATABASE.getDatabaseName() + "`");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE TABLE `t-1`");
        Assertions.assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE TABLE `t.2`");
    }

    private String getDdl(final List<SourceRecord> schemaChanges, int index) {
        return ((Struct) schemaChanges.get(index).value()).getString("ddl");
    }
}
