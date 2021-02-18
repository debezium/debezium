/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.zzz;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.MySqlTestConnection;
import io.debezium.connector.mysql.UniqueDatabase;
import io.debezium.connector.mysql.legacy.BinlogReaderIT;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * The test is named to make sure it runs alphabetically last as it can influence execution
 * of for example {@link BinlogReaderIT}
 *
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class ZZZGtidSetIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(DB_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = new UniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        RO_DATABASE.createAndInitialize();
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

    private boolean isGtidModeEnabled() throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            return db.queryAndMap(
                    "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                    rs -> {
                        if (rs.next()) {
                            return !"OFF".equalsIgnoreCase(rs.getString(2));
                        }
                        throw new IllegalStateException("Cannot obtain GTID status");
                    });
        }
    }

    @Test
    @FixFor("DBZ-1184")
    public void shouldProcessPurgedGtidSet() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        if (!isGtidModeEnabled()) {
            logger.warn("GTID is not enabled, skipping shouldProcessPurgedGtidSet");
            return;
        }

        purgeDatabaseLogs();
        final UniqueDatabase database = new UniqueDatabase("myServer1", "connector_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        final UniqueDatabase ro_database = new UniqueDatabase("myServer2", "connector_test_ro", database)
                .withDbHistoryPath(DB_HISTORY_PATH);
        ro_database.createAndInitialize();

        // Use the DB configuration to define the connector's configuration ...
        config = ro_database.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, ro_database.qualifiedTableName("customers"))
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(6 + 4); // 6 DDL changes
        assertThat(records.recordsForTopic(ro_database.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(ro_database.getDatabaseName()).size()).isEqualTo(6);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        // Check that records have GTID that does not contain purged interval
        records.recordsForTopic(ro_database.topicForTable("customers")).forEach(record -> {
            final String gtids = (String) record.sourceOffset().get("gtids");
            final Pattern p = Pattern.compile(".*(.*):(.*)-(.*).*");
            final Matcher m = p.matcher(gtids);
            m.matches();
            Assertions.assertThat(m.group(2)).isNotEqualTo("1");
        });

        stopConnector();
    }

    private void purgeDatabaseLogs() throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "FLUSH LOGS");
                final String lastBinlogName = connection.queryAndMap("SHOW BINARY LOGS", rs -> {
                    String binlog = null;
                    while (rs.next()) {
                        binlog = rs.getString(1);
                    }
                    return binlog;
                });
                connection.execute(
                        "PURGE BINARY LOGS TO '" + lastBinlogName + "'");
            }
        }
    }

    @Test
    @FixFor("DBZ-1244")
    public void shouldProcessPurgedLogsWhenDownAndSnapshotNeeded() throws SQLException, InterruptedException {
        Testing.Files.delete(DB_HISTORY_PATH);

        if (!isGtidModeEnabled()) {
            logger.warn("GTID is not enabled, skipping shouldProcessPurgedLogsWhenDownAndSnapshotNeeded");
            return;
        }

        purgeDatabaseLogs();
        final UniqueDatabase database = new UniqueDatabase("myServer1", "connector_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        database.createAndInitialize();

        // Use the DB configuration to define the connector's configuration ...
        config = database.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.WHEN_NEEDED)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName("customers"))
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1 + 3 + 2 * 4 + 4); // SET + DROP/CREATE/USE DB + DROP/CREATE 4 tables + 4 data
        assertThat(records.recordsForTopic(database.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(database.getDatabaseName()).size()).isEqualTo(11);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        stopConnector();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,1,1,1)",
                    "INSERT INTO customers VALUES(default,2,2,2)");
        }

        start(MySqlConnector.class, config);
        records = consumeRecordsByTopic(2);
        stopConnector();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,3,3,3)",
                    "INSERT INTO customers VALUES(default,4,4,4)");
        }
        purgeDatabaseLogs();
        start(MySqlConnector.class, config);
        // SET + DROP/CREATE/USE DB + DROP/CREATE 4 tables + 8 data
        records = consumeRecordsByTopic(1 + 3 + 2 * 4 + 8);
        assertThat(records.recordsForTopic(database.topicForTable("customers")).size()).isEqualTo(8);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(database.getDatabaseName()).size()).isEqualTo(11);
        stopConnector();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,5,5,5)",
                    "INSERT INTO customers VALUES(default,6,6,6)");
        }
        purgeDatabaseLogs();
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,7,7,7)",
                    "INSERT INTO customers VALUES(default,8,8,8)");
        }
        start(MySqlConnector.class, config);
        // SET + DROP/CREATE/USE DB + DROP/CREATE 4 tables + 8 data
        records = consumeRecordsByTopic(1 + 3 + 2 * 4 + 12);
        assertThat(records.recordsForTopic(database.topicForTable("customers")).size()).isEqualTo(12);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(database.getDatabaseName()).size()).isEqualTo(11);
        stopConnector();
    }
}
