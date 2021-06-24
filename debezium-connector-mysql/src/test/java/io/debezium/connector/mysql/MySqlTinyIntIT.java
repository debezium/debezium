/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.converters.TinyIntOneToBooleanConverter;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Verify correct range of TINYINT.
 *
 * @author Jiri Pechanec
 */
public class MySqlTinyIntIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-year.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("tinyintit", "tinyint_test")
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
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1773")
    public void shouldHandleTinyIntAsNumber() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DBZ1773"))
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        consumeInitial();

        assertIntChangeRecord();

        try (final Connection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO DBZ1773 VALUES (DEFAULT, 100, 5, 50, true)");
        }
        assertIntChangeRecord();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-1800")
    public void shouldHandleTinyIntOneAsBoolean() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DBZ1773"))
                .with(MySqlConnectorConfig.CUSTOM_CONVERTERS, "boolean")
                .with("boolean.type", TinyIntOneToBooleanConverter.class.getName())
                .with("boolean.selector", ".*DBZ1773.b")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        consumeInitial();

        assertBooleanChangeRecord();

        try (final Connection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO DBZ1773 VALUES (DEFAULT, 100, 5, 50, true)");
        }
        assertBooleanChangeRecord();

        stopConnector();
    }

    @Test
    @FixFor("DBZ-2085")
    public void shouldDefaultValueForTinyIntOneAsBoolean() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("DBZ2085"))
                .with(MySqlConnectorConfig.CUSTOM_CONVERTERS, "boolean")
                .with("boolean.type", TinyIntOneToBooleanConverter.class.getName())
                .with("boolean.selector", ".*DBZ2085.b")
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        consumeInitial();

        assertDefaultValueBooleanChangeRecord();

        try (final Connection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO DBZ2085 VALUES (DEFAULT, true)");
        }
        assertDefaultValueBooleanChangeRecord();

        stopConnector();
    }

    private void consumeInitial() throws InterruptedException {
        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        final int numDatabase = 2;
        final int numTables = 4;
        final int numOthers = 2;
        consumeRecords(numDatabase + numTables + numOthers);
    }

    private void assertIntChangeRecord() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.getInt16("ti")).isEqualTo((short) 100);
        Assertions.assertThat(change.getInt16("ti1")).isEqualTo((short) 5);
        Assertions.assertThat(change.getInt16("ti2")).isEqualTo((short) 50);
        Assertions.assertThat(change.getInt16("b")).isEqualTo((short) 1);
    }

    private void assertBooleanChangeRecord() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.getInt16("ti")).isEqualTo((short) 100);
        Assertions.assertThat(change.getInt16("ti1")).isEqualTo((short) 5);
        Assertions.assertThat(change.getInt16("ti2")).isEqualTo((short) 50);
        Assertions.assertThat(change.getBoolean("b")).isEqualTo(true);
    }

    private void assertDefaultValueBooleanChangeRecord() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        Assertions.assertThat(change.getBoolean("b")).isEqualTo(true);
        Assertions.assertThat(change.schema().field("b").schema().defaultValue()).isEqualTo(false);
    }
}
