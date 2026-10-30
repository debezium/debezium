/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;

/**
 * Verifies the incremental snapshot of a table declared with system versioning, see
 * <a href="https://github.com/debezium/dbz/issues/1317">debezium/dbz#1317</a>. Such a table has HIDDEN generated
 * columns. The JDBC driver reports them, so the schema of the table holds them, but SQL Server does not return them
 * for {@code SELECT *}. The table therefore has to be read with an explicit list of its columns, the same way the
 * initial snapshot reads it, otherwise the produced row is shorter than the schema and the connector fails with
 * "Data row is smaller than a column index, internal schema representation is probably out of sync with real database
 * schema".
 */
public class SqlServerHiddenColumnsIncrementalSnapshotIT extends AbstractAsyncEngineConnectorTest {

    private static final String HIDDEN_TABLE = "HiddenTest";
    private static final String PLAIN_TABLE = "PlainTest";
    private static final String HIDDEN_TABLE_TOPIC = "server1.testDB1.dbo." + HIDDEN_TABLE;
    private static final String PLAIN_TABLE_TOPIC = "server1.testDB1.dbo." + PLAIN_TABLE;
    private static final String HIDDEN_TABLE_PROJECTION = "[DeptID], [DeptName], [ManagerID], [ParentDeptID], "
            + "[SysStartTime], [SysEndTime]";
    private static final int ROW_COUNT = 3;
    private static final int CHUNK_SIZE = 250;
    private static final int SNAPSHOT_TIMEOUT_SECONDS = 120;

    private LogInterceptor chunkQueryLog;
    private LogInterceptor signalLog;

    @BeforeEach
    void before() throws Exception {
        TestHelper.createTestDatabase();
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.execute(
                    "CREATE TABLE dbo.HiddenTest ("
                            + "DeptID INT NOT NULL PRIMARY KEY CLUSTERED,"
                            + "DeptName VARCHAR(50) NOT NULL,"
                            + "ManagerID INT NULL,"
                            + "ParentDeptID INT NULL,"
                            + "SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,"
                            + "SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,"
                            + "PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)"
                            + ") WITH (SYSTEM_VERSIONING = ON)",
                    "INSERT INTO dbo.HiddenTest (DeptID, DeptName, ManagerID, ParentDeptID) VALUES "
                            + "(1, 'a', NULL, 2), (2, 'b', 3, 4), (3, 'c', NULL, 6)",
                    "CREATE TABLE dbo.PlainTest (Id INT NOT NULL PRIMARY KEY, Name VARCHAR(50) NOT NULL)",
                    "INSERT INTO dbo.PlainTest (Id, Name) VALUES (1, 'plain')",
                    "CREATE TABLE dbo.debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
            TestHelper.enableTableCdc(connection, HIDDEN_TABLE);
            TestHelper.enableTableCdc(connection, PLAIN_TABLE);
            TestHelper.enableTableCdc(connection, "debezium_signal");
            TestHelper.adjustCdcPollingInterval(connection, 1);
        }

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis());
    }

    @AfterEach
    void after() {
        stopConnector();
        TestHelper.dropTestDatabase();
        if (chunkQueryLog != null) {
            chunkQueryLog.setLoggerLevel(AbstractIncrementalSnapshotChangeEventSource.class, Level.INFO);
        }
    }

    @Test
    @FixFor("debezium/dbz#1317")
    void incrementalSnapshotOfTableWithHiddenColumns() throws Exception {
        final List<SourceRecord> records = incrementalSnapshot(HIDDEN_TABLE, HIDDEN_TABLE_TOPIC, CHUNK_SIZE, ROW_COUNT, config -> {
        });

        assertThat(failureMessages(signalLog)).noneMatch(message -> message.contains("Data row is smaller than a column index"));
        assertThat(records).hasSize(ROW_COUNT);
        // The HIDDEN columns are part of the schema of the table, so they are part of the payload as well
        final Struct after = ((Struct) records.get(0).value()).getStruct("after");
        assertThat(after.schema().field("SysStartTime")).isNotNull();
        assertThat(after.schema().field("SysEndTime")).isNotNull();

        // The chunk query projects the columns of the table explicitly, because SELECT * omits the HIDDEN columns
        assertThat(chunkQueryLog.getLogEntriesThatContainsMessage("using select statement"))
                .anyMatch(entry -> entry.contains("SELECT TOP " + CHUNK_SIZE + " " + HIDDEN_TABLE_PROJECTION + " FROM"));
    }

    @Test
    @FixFor("debezium/dbz#1317")
    void incrementalSnapshotOfTableWithHiddenColumnsInSeveralChunks() throws Exception {
        final List<SourceRecord> records = incrementalSnapshot(HIDDEN_TABLE, HIDDEN_TABLE_TOPIC, 1, ROW_COUNT, config -> {
        });

        assertThat(failureMessages(signalLog)).noneMatch(message -> message.contains("Data row is smaller than a column index"));
        // Every row of the table is emitted exactly once, although the table is read in one chunk per row
        assertThat(records).hasSize(ROW_COUNT);
        assertThat(records.stream().map(record -> ((Struct) record.value()).getStruct("after").getInt32("DeptID")).distinct())
                .containsExactlyInAnyOrder(1, 2, 3);

        // Every chunk uses the same explicit projection
        assertThat(chunkQueryLog.getLogEntriesThatContainsMessage("SELECT TOP 1 " + HIDDEN_TABLE_PROJECTION + " FROM"))
                .hasSizeGreaterThanOrEqualTo(ROW_COUNT);
        assertThat(chunkQueryLog.getLogEntriesThatContainsMessage("SELECT TOP 1 * FROM")).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#1317")
    void incrementalSnapshotOfTableWithHiddenColumnsWithColumnFilter() throws Exception {
        final List<SourceRecord> records = incrementalSnapshot(HIDDEN_TABLE, HIDDEN_TABLE_TOPIC, CHUNK_SIZE, ROW_COUNT,
                config -> config.with(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo." + HIDDEN_TABLE + ".ManagerID"));

        assertThat(failureMessages(signalLog)).noneMatch(message -> message.contains("Data row is smaller than a column index"));
        assertThat(records).hasSize(ROW_COUNT);
        // The excluded column is not part of the schema of the table, so it is not part of the payload either
        final Struct after = ((Struct) records.get(0).value()).getStruct("after");
        assertThat(after.schema().field("ManagerID")).isNull();
        assertThat(after.schema().field("SysStartTime")).isNotNull();

        // The projection is the filtered column list
        assertThat(chunkQueryLog.getLogEntriesThatContainsMessage("using select statement"))
                .anyMatch(entry -> entry.contains("SELECT TOP " + CHUNK_SIZE
                        + " [DeptID], [DeptName], [ParentDeptID], [SysStartTime], [SysEndTime] FROM"));
    }

    /**
     * Control: the incremental snapshot of a table without HIDDEN columns still uses {@code SELECT *}.
     */
    @Test
    @FixFor("debezium/dbz#1317")
    void incrementalSnapshotOfTableWithoutHiddenColumns() throws Exception {
        final List<SourceRecord> records = incrementalSnapshot(PLAIN_TABLE, PLAIN_TABLE_TOPIC, CHUNK_SIZE, 1, config -> {
        });

        assertThat(failureMessages(signalLog)).noneMatch(message -> message.contains("Data row is smaller than a column index"));
        assertThat(records).hasSize(1);
        assertThat(chunkQueryLog.getLogEntriesThatContainsMessage("using select statement"))
                .anyMatch(entry -> entry.contains("SELECT TOP " + CHUNK_SIZE + " * FROM"));
    }

    /**
     * Runs an incremental snapshot of the given table and collects the records of the given topic.
     *
     * @param table the table to snapshot
     * @param topic the topic the records of the table are published to
     * @param chunkSize the number of rows read by a single chunk
     * @param expectedRecords the number of records the snapshot is expected to emit
     * @param customizer customizes the configuration of the connector
     * @return the records of the topic
     */
    private List<SourceRecord> incrementalSnapshot(String table, String topic, int chunkSize, int expectedRecords,
                                                   Consumer<Configuration.Builder> customizer)
            throws Exception {
        chunkQueryLog = new LogInterceptor(AbstractIncrementalSnapshotChangeEventSource.class);
        chunkQueryLog.setLoggerLevel(AbstractIncrementalSnapshotChangeEventSource.class, Level.DEBUG);
        signalLog = new LogInterceptor(SignalProcessor.class);

        final Configuration.Builder config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo." + HIDDEN_TABLE + ",dbo." + PLAIN_TABLE + ",dbo.debezium_signal")
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.TEST_DATABASE_1 + ".dbo.debezium_signal")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, chunkSize)
                .withDefault(RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.MICROSECONDS);
        customizer.accept(config);

        start(SqlServerConnector.class, config.build());
        assertConnectorIsRunning();

        TestHelper.waitForStreamingStarted();

        // Trigger the incremental snapshot through the source signal channel
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.execute("INSERT INTO dbo.debezium_signal VALUES ('signal-1', 'execute-snapshot', "
                    + "'{\"type\": \"INCREMENTAL\", \"data-collections\": [\"" + TestHelper.TEST_DATABASE_1 + ".dbo." + table + "\"]}')");
        }

        final List<SourceRecord> records = new ArrayList<>();
        await().atMost(SNAPSHOT_TIMEOUT_SECONDS, TimeUnit.SECONDS).until(() -> {
            consumeAvailableRecords(record -> {
                if (record.topic().equals(topic)) {
                    records.add(record);
                }
            });
            return records.size() >= expectedRecords || failedOnRowLength(signalLog);
        });
        return records;
    }

    /**
     * Checks whether the snapshot failed because the row read was shorter than the schema of the table.
     */
    private static boolean failedOnRowLength(LogInterceptor logInterceptor) {
        return failureMessages(logInterceptor).stream().anyMatch(message -> message.contains("Data row is smaller than a column index"));
    }

    /**
     * Collects the messages of the throwable chains of all logged events.
     */
    private static List<String> failureMessages(LogInterceptor logInterceptor) {
        final List<String> messages = new ArrayList<>();
        for (ILoggingEvent event : logInterceptor.getLoggingEvents("")) {
            IThrowableProxy throwable = event.getThrowableProxy();
            while (throwable != null) {
                if (throwable.getMessage() != null) {
                    messages.add(throwable.getMessage());
                }
                throwable = throwable.getCause();
            }
        }
        return messages;
    }
}
