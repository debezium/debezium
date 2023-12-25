/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Inki Hwang
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class MySqlConnectorSchemaValidateIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("sql_bin_log_off", "sql_bin_log_off_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    private static final int INITIAL_EVENT_COUNT = 6;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-7093")
    public void shouldRecoverToSyncSchemaWhenAddColumnToEndWithSqlLogBinIsOff() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        alterTableWithSqlBinLogOff("ALTER TABLE dbz7093 ADD newcol VARCHAR(20);", replicaIsMaster);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO dbz7093(id, age, name, newcol) VALUES (201, 1,'name1','newcol1');");
                connection.execute("UPDATE dbz7093 SET age=2, name='name2', newcol='newcol2' WHERE id=201");
                connection.execute("DELETE FROM dbz7093 WHERE id=201");
            }
        }

        waitForConnectorShutdown("mysql", DATABASE.getServerName());
        stopConnector();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }

        Files.delete(DB_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY_RECOVERY)
                .build();

        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        // recover initial event
        records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("dbz7093"));
        assertThat(recordsForTopic.size()).isEqualTo(4);
        SourceRecord insertEvent = recordsForTopic.get(0);
        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/age", 1);
        assertValueField(insertEvent, "after/name", "name1");
        assertValueField(insertEvent, "after/newcol", "newcol1");

        SourceRecord updateEvent = recordsForTopic.get(1);
        assertUpdate(updateEvent, "id", 201);
        assertValueField(updateEvent, "before/age", 1);
        assertValueField(updateEvent, "before/name", "name1");
        assertValueField(updateEvent, "before/newcol", "newcol1");
        assertValueField(updateEvent, "after/age", 2);
        assertValueField(updateEvent, "after/name", "name2");
        assertValueField(updateEvent, "after/newcol", "newcol2");

        SourceRecord deleteEvent = recordsForTopic.get(2);
        assertDelete(deleteEvent, "id", 201);
        assertValueField(deleteEvent, "before/age", 2);
        assertValueField(deleteEvent, "before/name", "name2");
        assertValueField(deleteEvent, "before/newcol", "newcol2");

        SourceRecord tombstoneEvent = recordsForTopic.get(3);
        assertTombstone(tombstoneEvent);
    }

    @Test
    @FixFor("DBZ-7093")
    public void shouldRecoverToSyncSchemaWhenAddColumnInMiddleWithSqlLogBinIsOff() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        alterTableWithSqlBinLogOff("ALTER TABLE dbz7093 ADD newcol VARCHAR(20) AFTER age;", replicaIsMaster);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO dbz7093(id, age, name, newcol) VALUES (201, 1,'name1','newcol1');");
                connection.execute("UPDATE dbz7093 SET age=2, name='name2', newcol='newcol2' WHERE id=201");
                connection.execute("DELETE FROM dbz7093 WHERE id=201");
            }
        }

        waitForConnectorShutdown("mysql", DATABASE.getServerName());
        stopConnector();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }

        Files.delete(DB_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY_RECOVERY)
                .build();

        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        // recover initial event
        records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("dbz7093"));
        assertThat(recordsForTopic.size()).isEqualTo(4);
        SourceRecord insertEvent = recordsForTopic.get(0);
        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/age", 1);
        assertValueField(insertEvent, "after/name", "name1");
        assertValueField(insertEvent, "after/newcol", "newcol1");

        SourceRecord updateEvent = recordsForTopic.get(1);
        assertUpdate(updateEvent, "id", 201);
        assertValueField(updateEvent, "before/age", 1);
        assertValueField(updateEvent, "before/name", "name1");
        assertValueField(updateEvent, "before/newcol", "newcol1");
        assertValueField(updateEvent, "after/age", 2);
        assertValueField(updateEvent, "after/name", "name2");
        assertValueField(updateEvent, "after/newcol", "newcol2");

        SourceRecord deleteEvent = recordsForTopic.get(2);
        assertDelete(deleteEvent, "id", 201);
        assertValueField(deleteEvent, "before/age", 2);
        assertValueField(deleteEvent, "before/name", "name2");
        assertValueField(deleteEvent, "before/newcol", "newcol2");

        SourceRecord tombstoneEvent = recordsForTopic.get(3);
        assertTombstone(tombstoneEvent);
    }

    @Test
    @FixFor("DBZ-7093")
    public void shouldRecoverToSyncSchemaWhenDropColumnWithSqlLogBinIsOff() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        alterTableWithSqlBinLogOff("ALTER TABLE dbz7093 DROP age;", replicaIsMaster);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO dbz7093(id, name) VALUES (201, 'name1');");
                connection.execute("UPDATE dbz7093 SET name='name2' WHERE id=201;");
                connection.execute("DELETE FROM dbz7093 WHERE id=201;");
            }
        }

        waitForConnectorShutdown("mysql", DATABASE.getServerName());
        stopConnector();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }

        Files.delete(DB_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY_RECOVERY)
                .build();

        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        // recover initial event
        records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("dbz7093"));
        assertThat(recordsForTopic.size()).isEqualTo(4);
        SourceRecord insertEvent = recordsForTopic.get(0);
        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/name", "name1");

        SourceRecord updateEvent = recordsForTopic.get(1);
        assertUpdate(updateEvent, "id", 201);
        assertValueField(updateEvent, "before/name", "name1");
        assertValueField(updateEvent, "after/name", "name2");

        SourceRecord deleteEvent = recordsForTopic.get(2);
        assertDelete(deleteEvent, "id", 201);
        assertValueField(deleteEvent, "before/name", "name2");

        SourceRecord tombstoneEvent = recordsForTopic.get(3);
        assertTombstone(tombstoneEvent);
    }

    @Test
    @FixFor("DBZ-7093")
    public void shouldRecoverToSyncSchemaWhenAddColumnToEndWithSqlLogBinIsOffAndColumnInclude() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.COLUMN_INCLUDE_LIST, "dbz7093.id" + "," + "dbz7093.newcol")
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        alterTableWithSqlBinLogOff("ALTER TABLE dbz7093 ADD newcol VARCHAR(20);", replicaIsMaster);

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO dbz7093(id, age, name, newcol) VALUES (201, 1,'name1','newcol1');");
                connection.execute("UPDATE dbz7093 SET newcol='newcol2' WHERE id=201;");
                connection.execute("DELETE FROM dbz7093 WHERE id=201;");
            }
        }

        waitForConnectorShutdown("mysql", DATABASE.getServerName());
        stopConnector();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }

        Files.delete(DB_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY_RECOVERY)
                .build();

        start(MySqlConnector.class, config, (success, message, error) -> exception.set(error));

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        // recover initial event
        records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);
        assertThat(records.allRecordsInOrder().size()).isEqualTo(6);

        records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("dbz7093"));
        assertThat(recordsForTopic.size()).isEqualTo(4);

        SourceRecord insertEvent = recordsForTopic.get(0);
        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/newcol", "newcol1");

        SourceRecord updateEvent = recordsForTopic.get(1);
        assertUpdate(updateEvent, "id", 201);
        assertValueField(updateEvent, "before/newcol", "newcol1");
        assertValueField(updateEvent, "after/newcol", "newcol2");

        SourceRecord deleteEvent = recordsForTopic.get(2);
        assertDelete(deleteEvent, "id", 201);
        assertValueField(deleteEvent, "before/newcol", "newcol2");

        SourceRecord tombstoneEvent = recordsForTopic.get(3);
        assertTombstone(tombstoneEvent);
    }

    private void alterTableWithSqlBinLogOff(String ddl, boolean replicaIsMaster) throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("SET SQL_LOG_BIN=OFF;");
                // debezium couldn't notice table changed because this DDL is not recorded in binlog
                connection.execute(ddl);
                connection.execute("SET SQL_LOG_BIN=ON;");
            }
        }

        if (!replicaIsMaster) {
            // if has replica, also apply DDL because master didn't record DDL at binlog
            try (MySqlTestConnection db = MySqlTestConnection.forTestReplicaDatabase(DATABASE.getDatabaseName())) {
                try (JdbcConnection connection = db.connect()) {
                    connection.execute("SET SQL_LOG_BIN=OFF;");
                    connection.execute(ddl);
                    connection.execute("SET SQL_LOG_BIN=ON;");
                }
            }
        }
    }
}
