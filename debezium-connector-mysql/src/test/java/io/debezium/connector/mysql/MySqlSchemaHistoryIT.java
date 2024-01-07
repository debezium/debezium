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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
*
* The test to verify whether DDL is stored correctly in database schema history.
*
* @author Jiri Pechanec
*/
public class MySqlSchemaHistoryIT extends AbstractAsyncEngineConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();

    private static final int TABLE_COUNT = 2;

    private UniqueDatabase DATABASE;

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE = new UniqueDatabase("history", "history-dbz")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        DATABASE.createAndInitialize();

        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
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
        assertThat(getDdl(schemaChanges, 0)).startsWith("RENAME TABLE `t-1` TO `new-t-1`");

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
        assertThat(getDdl(schemaChanges, 0)).startsWith("RENAME TABLE `t-1` TO `new-t-1`");
        assertThat(getDdl(schemaChanges, 1)).startsWith("RENAME TABLE `t.2` TO `new.t.2`");

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
        assertThat(getDdl(schemaChanges, 0)).startsWith("ALTER TABLE `t-1` RENAME TO `new-t-1`");

        stopConnector();

        start(MySqlConnector.class, config);
        assertConnectorIsRunning();
        stopConnector();
    }

    private void assertDdls(SourceRecords records) {
        final List<SourceRecord> schemaChanges = records.recordsForTopic(DATABASE.getServerName());
        int index = 0;
        assertThat(getDdl(schemaChanges, index++)).startsWith("SET");
        assertThat(getDdl(schemaChanges, index++)).startsWith("DROP TABLE IF EXISTS `" + DATABASE.getDatabaseName() + "`.`t-1`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("DROP TABLE IF EXISTS `" + DATABASE.getDatabaseName() + "`.`t.2`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("DROP DATABASE IF EXISTS `" + DATABASE.getDatabaseName() + "`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE DATABASE `" + DATABASE.getDatabaseName() + "`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("USE `" + DATABASE.getDatabaseName() + "`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE TABLE `t-1`");
        assertThat(getDdl(schemaChanges, index++)).startsWith("CREATE TABLE `t.2`");
    }

    private String getDdl(final List<SourceRecord> schemaChanges, int index) {
        return ((Struct) schemaChanges.get(index).value()).getString("ddl");
    }
}
