/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.antlr.listener.RenameTableParserListener;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec
 */
public class MySqlSchemaMigrationIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-json.txt")
            .toAbsolutePath();
    private UniqueDatabase DATABASE = new UniqueDatabase("migration", "empty")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
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
    public void shouldCorrectlyMigrateTable() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        // Breaks if table whitelist does not contain both tables
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("monitored") + "," + DATABASE.qualifiedTableName("_monitored_new"))
                .build();

        final MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        connection.execute("create table monitored (id int auto_increment primary key, value1 varchar(100), value2 int)");
        connection.execute("insert into monitored values(default, 'a1', 1)");

        // Start the connector ...
        start(MySqlConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();

        connection.execute("insert into monitored values(default, 'a2', 2)");
        connection.execute(
                "CREATE TABLE `_monitored_new` ( `id` int(11) NOT NULL AUTO_INCREMENT, `value1` varchar(100) DEFAULT NULL, `value2` int(11) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1");
        connection.execute("ALTER TABLE `_monitored_new` drop value1");
        connection.execute("insert into _monitored_new values(default, 1)");
        connection.execute("insert into _monitored_new values(default, 2)");
        connection.execute("RENAME TABLE `monitored` TO `_monitored_old`, `_monitored_new` TO `monitored`");
        connection.execute("insert into monitored values(default, 3)");

        records = consumeRecordsByTopic(4);
        stopConnector();
        assertThat(records).isNotNull();
        records.forEach(this::validate);

        assertInsert(records.allRecordsInOrder().get(3), "id", 5);
    }

    @Test
    public void shouldProcessAndWarnOnNonWhitelistedMigrateTable() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        final LogInterceptor logInterceptor = new LogInterceptor(RenameTableParserListener.class);

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("monitored"))
                .build();

        final MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        connection.execute("create table monitored (id int auto_increment primary key, value1 varchar(100), value2 int)");
        connection.execute("insert into monitored values(default, 'a1', 1)");

        // Start the connector ...
        start(MySqlConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();

        connection.execute("insert into monitored values(default, 'a2', 2)");
        connection.execute(
                "CREATE TABLE `_monitored_new` ( `id` int(11) NOT NULL AUTO_INCREMENT, `value1` varchar(100) DEFAULT NULL, `value2` int(11) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1");
        connection.execute("ALTER TABLE `_monitored_new` drop value1");
        connection.execute("insert into _monitored_new values(default, 1)");
        connection.execute("insert into _monitored_new values(default, 2)");
        connection.execute("RENAME TABLE `monitored` TO `_monitored_old`, `_monitored_new` TO `monitored`");
        connection.execute("insert into monitored values(default, 3)");

        final String msg1 = "Renaming included table " + DATABASE.qualifiedTableName("monitored") + " to non-included table "
                + DATABASE.qualifiedTableName("_monitored_old") + ", this can lead to schema inconsistency";
        final String msg2 = "Renaming non-included table " + DATABASE.qualifiedTableName("_monitored_new") + " to included table "
                + DATABASE.qualifiedTableName("monitored") + ", this can lead to schema inconsistency";

        records = consumeRecordsByTopic(2);
        stopConnector(value -> {
            assertThat(logInterceptor.containsWarnMessage(msg1)).isTrue();
            assertThat(logInterceptor.containsWarnMessage(msg2)).isTrue();
        });

        assertThat(records).isNotNull();
        records.forEach(this::validate);

        assertInsert(records.allRecordsInOrder().get(1), "id", 5);
    }

    @Test
    public void shouldWarnOnInvalidMigrateTable() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        final LogInterceptor logInterceptor = new LogInterceptor(RenameTableParserListener.class);

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("monitored"))
                .build();

        final MySqlTestConnection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        connection.execute("create table monitored (id int auto_increment primary key, value1 varchar(100), value2 int)");
        connection.execute("insert into monitored values(default, 'a1', 1)");

        // Start the connector ...
        start(MySqlConnector.class, config);
        SourceRecords records = consumeRecordsByTopic(1);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();

        connection.execute("insert into monitored values(default, 'a2', 2)");
        connection.execute(
                "CREATE TABLE `_monitored_new` ( `id` int(11) NOT NULL AUTO_INCREMENT, `value1` varchar(100) DEFAULT NULL, `value2` int(11) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1");
        connection.execute("ALTER TABLE `_monitored_new` drop value1");
        connection.execute("insert into _monitored_new values(default, 1)");
        connection.execute("insert into _monitored_new values(default, 2)");
        connection.execute("RENAME TABLE `monitored` TO `_monitored_old`, `_monitored_new` TO `monitored`");
        connection.execute("insert into monitored values(default, 3)");

        final String msg1 = "Renaming included table " + DATABASE.qualifiedTableName("monitored") + " to non-included table "
                + DATABASE.qualifiedTableName("_monitored_old") + ", this can lead to schema inconsistency";
        final String msg2 = "Renaming non-included table " + DATABASE.qualifiedTableName("_monitored_new") + " to included table "
                + DATABASE.qualifiedTableName("monitored") + ", this can lead to schema inconsistency";

        records = consumeRecordsByTopic(2);
        stopConnector(value -> {
            assertThat(logInterceptor.containsWarnMessage(msg1)).isTrue();
            assertThat(logInterceptor.containsWarnMessage(msg2)).isTrue();
        });

        assertThat(records).isNotNull();
        records.forEach(this::validate);

        assertInsert(records.allRecordsInOrder().get(1), "id", 5);
    }
}
