/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Inki Hwang
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public abstract class BinlogConvertingFailureIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path DB_HISTORY_PATH = Files.createTestingPath("file-db-history-converting-failure.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("converting_failure", "converting_failure")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    // 4 meta events (set character_set etc.) and then 2 tables with 2 events each (drop DDL, create DDL)
    private static final int INITIAL_EVENT_COUNT = 4 + (2 * 2);

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

        dropAllDatabases();
    }

    @Test
    @FixFor("DBZ-7143")
    public void shouldRecoverToSyncSchemaWhenFailedValueConvertByDdlWithSqlLogBinIsOff() throws Exception {
        // Use the DB configuration to define the connector's configuration to use the "replica"
        // which may be the same as the "master" ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz7143"))
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, EventConvertingFailureHandlingMode.FAIL)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        alterTableWithSqlBinLogOff("ALTER TABLE dbz7143 MODIFY COLUMN age VARCHAR(200);", replicaIsMaster);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO dbz7143(id, name, age) VALUES (201, 'name1', 'age1');");
                connection.execute("UPDATE dbz7143 SET age='age2' WHERE id=201;");
                connection.execute("DELETE FROM dbz7143 WHERE id=201;");
            }
        }

        waitForConnectorShutdown(getConnectorName(), DATABASE.getServerName());
        waitForEngineShutdown();
        cleanupTestFwkState();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }

        Files.delete(DB_HISTORY_PATH);

        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.replica.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.replica.port", "3306"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.RECOVERY)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz7143"))
                .build();

        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);

        // recover initial event
        records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);

        records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("dbz7143"));
        assertThat(recordsForTopic.size()).isEqualTo(4);
        SourceRecord insertEvent = recordsForTopic.get(0);
        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/age", "age1");

        SourceRecord updateEvent = recordsForTopic.get(1);
        assertUpdate(updateEvent, "id", 201);
        assertValueField(updateEvent, "before/age", "age1");
        assertValueField(updateEvent, "after/age", "age2");

        SourceRecord deleteEvent = recordsForTopic.get(2);
        assertDelete(deleteEvent, "id", 201);
        assertValueField(deleteEvent, "before/age", "age2");

        SourceRecord tombstoneEvent = recordsForTopic.get(3);
        assertTombstone(tombstoneEvent);
    }

    @Test
    @FixFor("DBZ-7143")
    public void shouldFailConversionNullableTimeTypeWithConnectModeWhenWarnMode() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("time_table"))
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, EventConvertingFailureHandlingMode.WARN)
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // CONNECT mode should be in range from 00:00:00 to 24:00:00
                // the values is replaced to null with WARN mode.
                connection.execute("INSERT INTO time_table VALUES (201, '-23:45:56.7', '123:00:00.123456', '23:45:56.0');");
            }
        }

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);

        records = consumeRecordsByTopic(1);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("time_table"));
        SourceRecord insertEvent = recordsForTopic.get(0);

        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/A", null);
        assertValueField(insertEvent, "after/B", null);
    }

    @Test
    @FixFor("DBZ-7143")
    public void shouldFailedConvertedValueIsNullWithSkipMode() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz7143"))
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, EventConvertingFailureHandlingMode.SKIP)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        String masterPort = System.getProperty("database.port", "3306");
        String replicaPort = System.getProperty("database.replica.port", "3306");
        boolean replicaIsMaster = masterPort.equals(replicaPort);
        if (!replicaIsMaster) {
            // Give time for the replica to catch up to the master ...
            Thread.sleep(5000L);
        }

        alterTableWithSqlBinLogOff("ALTER TABLE dbz7143 MODIFY COLUMN age VARCHAR(200);", replicaIsMaster);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("INSERT INTO dbz7143(id, name, age) VALUES (201, 'name1', 'age1');");
                connection.execute("UPDATE dbz7143 SET age='age2' WHERE id=201;");
                connection.execute("DELETE FROM dbz7143 WHERE id=201;");
            }
        }

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);

        records = consumeRecordsByTopic(4);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("dbz7143"));
        assertThat(recordsForTopic.size()).isEqualTo(4);
        SourceRecord insertEvent = recordsForTopic.get(0);
        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/age", null);

        SourceRecord updateEvent = recordsForTopic.get(1);
        assertUpdate(updateEvent, "id", 201);
        assertValueField(updateEvent, "before/age", null);
        assertValueField(updateEvent, "after/age", null);

        SourceRecord deleteEvent = recordsForTopic.get(2);
        assertDelete(deleteEvent, "id", 201);
        assertValueField(deleteEvent, "before/age", null);

        SourceRecord tombstoneEvent = recordsForTopic.get(3);
        assertTombstone(tombstoneEvent);
    }

    @Test
    @FixFor("DBZ-7143")
    public void shouldFailConversionNotNullTimeTypeWithConnectModeWhenWarnMode() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("time_table"))
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, EventConvertingFailureHandlingMode.WARN)
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // CONNECT mode should be in range from 00:00:00 to 24:00:00
                // column C is NOT NULL and the value is out of range
                // then it throws exception despite WARN mode.
                connection.execute("INSERT INTO time_table VALUES (201, '23:45:56.7', '23:00:00.123456', '-23:45:56.0');");
            }
        }

        waitForConnectorShutdown(getConnectorName(), DATABASE.getServerName());
        waitForEngineShutdown();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }
    }

    @Test
    @FixFor("DBZ-7143")
    public void shouldFailConversionTimeTypeWithConnectModeWhenFailMode() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("time_table"))
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, EventConvertingFailureHandlingMode.FAIL)
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // CONNECT mode should be in range from 00:00:00 to 24:00:00
                // it throws exception by FAIL mode.
                connection.execute("INSERT INTO time_table VALUES (201, '-23:45:56.7', '123:00:00.123456', '23:45:56.0');");
            }
        }

        waitForConnectorShutdown(getConnectorName(), DATABASE.getServerName());
        waitForEngineShutdown();

        final Throwable e = exception.get();
        if (e == null) {
            // it should be thrown
            fail();
        }
    }

    @Test
    @FixFor("DBZ-7143")
    public void shouldFailConversionDefaultTimeTypeWithConnectModeWhenWarnMode() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("default_time_table"))
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, EventConvertingFailureHandlingMode.WARN)
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .build();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                // CONNECT mode should be in range from 00:00:00 to 24:00:00
                // the values of default is replaced to null with WARN mode.
                connection.execute(
                        "CREATE TABLE default_time_table (id INT NOT NULL, A TIME(1) DEFAULT '-23:45:56.7', B TIME(6) DEFAULT '123:00:00.123456', C TIME(1) NULL, PRIMARY KEY(id));");
                connection.execute("INSERT INTO default_time_table VALUES (201, DEFAULT, DEFAULT, DEFAULT);");
            }
        }

        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // origin initial event
        SourceRecords records = consumeRecordsByTopic(INITIAL_EVENT_COUNT);

        records = consumeRecordsByTopic(2);
        final List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("default_time_table"));
        SourceRecord insertEvent = recordsForTopic.get(0);

        assertInsert(insertEvent, "id", 201);
        assertValueField(insertEvent, "after/A", null);
        assertValueField(insertEvent, "after/B", null);
        assertValueField(insertEvent, "after/C", null);
    }

    private void alterTableWithSqlBinLogOff(String ddl, boolean replicaIsMaster) throws SQLException {
        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("SET SQL_LOG_BIN=OFF;");
                // debezium couldn't notice table changed because this DDL is not recorded in binlog
                connection.execute(ddl);
                connection.execute("SET SQL_LOG_BIN=ON;");
            }
        }

        if (!replicaIsMaster) {
            // if it has replica, also apply the DDL because master didn't record DDL at binlog
            try (BinlogTestConnection db = getTestReplicaDatabaseConnection(DATABASE.getDatabaseName())) {
                try (JdbcConnection connection = db.connect()) {
                    connection.execute("SET SQL_LOG_BIN=OFF;");
                    connection.execute(ddl);
                    connection.execute("SET SQL_LOG_BIN=ON;");
                }
            }
        }
    }
}
