/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.zzz;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.source.SourceConnector;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.junit.SkipTestDependingOnDatabaseRule;
import io.debezium.connector.binlog.junit.SkipTestDependingOnGtidModeRule;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs;
import io.debezium.connector.binlog.junit.SkipWhenGtidModeIs;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * The test is named to make sure it runs alphabetically last as it can influence execution of other tests.
 *
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
@SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "MariaDB does not support purged GTID sets")
@SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF)
public abstract class ZZZBinlogGtidSetIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);
    private final UniqueDatabase RO_DATABASE = TestHelper.getUniqueDatabase("myServer2", "connector_test_ro", DATABASE)
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Rule
    public TestRule skipTest = new SkipTestDependingOnGtidModeRule();
    @Rule
    public TestRule skipTest2 = new SkipTestDependingOnDatabaseRule();

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    private boolean isGtidModeEnabled() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            return db.isGtidEnabled();
        }
    }

    @Test
    @FixFor("DBZ-1184")
    public void shouldProcessPurgedGtidSet() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        purgeDatabaseLogs();
        final UniqueDatabase database = TestHelper.getUniqueDatabase("myServer1", "connector_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        final UniqueDatabase ro_database = TestHelper.getUniqueDatabase("myServer2", "connector_test_ro", database)
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        ro_database.createAndInitialize();

        // Use the DB configuration to define the connector's configuration ...
        config = ro_database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, ro_database.qualifiedTableName("customers"))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

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

            // the format is <uuid:<start-tx>-<end-tx>; we don't expect any offsets with start tx = 1 due to the flush
            final Pattern p = Pattern.compile(".*:(.*)-.*");
            final Matcher m = p.matcher(gtids);
            m.matches();
            assertThat(m.group(1)).isNotEqualTo("1");
        });

        stopConnector();
    }

    private void purgeDatabaseLogs() throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
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

    @Test
    @FixFor("DBZ-1244")
    public void shouldProcessPurgedLogsWhenDownAndSnapshotNeeded() throws SQLException, InterruptedException {
        Files.delete(SCHEMA_HISTORY_PATH);

        purgeDatabaseLogs();
        final UniqueDatabase database = TestHelper.getUniqueDatabase("myServer1", "connector_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        database.createAndInitialize();

        // Use the DB configuration to define the connector's configuration ...
        config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.WHEN_NEEDED)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName("customers"))
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();
        SourceRecords records = consumeRecordsByTopic(1 + 3 + 2 * 4 + 4); // SET + DROP/CREATE/USE DB + DROP/CREATE 4 tables + 4 data
        assertThat(records.recordsForTopic(database.topicForTable("customers")).size()).isEqualTo(4);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(database.getDatabaseName()).size()).isEqualTo(11);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);

        stopConnector();

        try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,1,1,1)",
                    "INSERT INTO customers VALUES(default,2,2,2)");
        }

        start(getConnectorClass(), config);
        records = consumeRecordsByTopic(2);
        stopConnector();

        try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,3,3,3)",
                    "INSERT INTO customers VALUES(default,4,4,4)");
        }
        purgeDatabaseLogs();
        start(getConnectorClass(), config);
        // SET + DROP/CREATE/USE DB + DROP/CREATE 4 tables + 8 data
        records = consumeRecordsByTopic(1 + 3 + 2 * 4 + 8);
        assertThat(records.recordsForTopic(database.topicForTable("customers")).size()).isEqualTo(8);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(database.getDatabaseName()).size()).isEqualTo(11);
        stopConnector();

        try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,5,5,5)",
                    "INSERT INTO customers VALUES(default,6,6,6)");
        }
        purgeDatabaseLogs();
        try (BinlogTestConnection db = getTestDatabaseConnection(database.getDatabaseName())) {
            db.execute(
                    "INSERT INTO customers VALUES(default,7,7,7)",
                    "INSERT INTO customers VALUES(default,8,8,8)");
        }
        start(getConnectorClass(), config);
        // SET + DROP/CREATE/USE DB + DROP/CREATE 4 tables + 8 data
        records = consumeRecordsByTopic(1 + 3 + 2 * 4 + 12);
        assertThat(records.recordsForTopic(database.topicForTable("customers")).size()).isEqualTo(12);
        assertThat(records.topics().size()).isEqualTo(1 + 1);
        assertThat(records.ddlRecordsForDatabase(database.getDatabaseName()).size()).isEqualTo(11);
        stopConnector();
    }
}
