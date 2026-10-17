/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Incremental snapshot of a {@code datetime} primary key must emit each row once. SQL Server
 * stores {@code datetime} in 1/300s ticks, so {@code .003} is {@code .00333...} and {@code .007}
 * is {@code .00666...}. Binding chunk bounds as {@code datetime2} made a {@code .003} key compare
 * greater than the parameter and could repeat a chunk-end row or drop a {@code .003} maximum key.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2871">debezium/dbz#2871</a>
 */
public class SqlServerDatetimePkIncrementalSnapshotIT extends AbstractAsyncEngineConnectorTest {

    private static final int POLLING_INTERVAL = 1;
    private static final int CHUNK_SIZE = 2;
    private static final int DT_PK_ROW_COUNT = 10;
    private static final int DT_MAX_ROW_COUNT = 5;

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE dt_pk (pk datetime primary key, id int)",
                "CREATE TABLE dt_max_003 (pk datetime primary key, id int)",
                "CREATE TABLE dt_max_007 (pk datetime primary key, id int)",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        connection.execute(
                "INSERT INTO dt_pk VALUES ('2025-01-15 01:00:00.000', 0)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 02:00:00.000', 1)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 02:30:00.000', 2)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 02:30:00.003', 3)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 02:30:00.007', 4)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 03:00:00.000', 5)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 03:00:00.003', 6)",
                "INSERT INTO dt_pk VALUES ('2025-01-15 03:00:00.007', 7)",
                "INSERT INTO dt_pk VALUES ('2025-01-16 01:30:00.003', 8)",
                "INSERT INTO dt_pk VALUES ('2025-01-17 00:00:00.000', 9)",
                "INSERT INTO dt_max_003 VALUES ('2025-01-01 00:00:00.000', 0)",
                "INSERT INTO dt_max_003 VALUES ('2025-01-01 00:00:01.000', 1)",
                "INSERT INTO dt_max_003 VALUES ('2025-01-01 00:00:02.000', 2)",
                "INSERT INTO dt_max_003 VALUES ('2025-01-01 00:00:03.000', 3)",
                "INSERT INTO dt_max_003 VALUES ('2025-01-01 00:00:04.003', 4)",
                "INSERT INTO dt_max_007 VALUES ('2025-01-01 00:00:00.000', 0)",
                "INSERT INTO dt_max_007 VALUES ('2025-01-01 00:00:01.000', 1)",
                "INSERT INTO dt_max_007 VALUES ('2025-01-01 00:00:02.000', 2)",
                "INSERT INTO dt_max_007 VALUES ('2025-01-01 00:00:03.000', 3)",
                "INSERT INTO dt_max_007 VALUES ('2025-01-01 00:00:04.007', 4)");
        TestHelper.enableTableCdc(connection, "dt_pk");
        TestHelper.enableTableCdc(connection, "dt_max_003");
        TestHelper.enableTableCdc(connection, "dt_max_007");
        TestHelper.enableTableCdc(connection, "debezium_signal");
        TestHelper.adjustCdcPollingInterval(connection, POLLING_INTERVAL);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis());
    }

    @AfterEach
    void after() throws SQLException {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2871")
    void incrementalSnapshotEmitsEachDatetimePkRowOnce() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, CHUNK_SIZE)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.dt_pk,dbo.dt_max_003,dbo.dt_max_007,dbo.debezium_signal")
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);
        TestHelper.waitForStreamingStarted();

        connection.execute(
                "INSERT INTO debezium_signal VALUES ('signal-1', 'execute-snapshot', "
                        + "'{\"data-collections\": [\"testDB1.dbo.dt_pk\", \"testDB1.dbo.dt_max_003\", \"testDB1.dbo.dt_max_007\"]}')");

        // Chunk size 2 emits open/close watermarks between chunks and tables, so a fixed
        // record count stops mid-table. Wait until each table has its expected unique ids.
        final Map<String, List<Integer>> idsByTable = new HashMap<>();
        consumeRecordsUntil(
                (consumed, record) -> hasExpectedSnapshots(idsByTable),
                (consumed, record) -> "Consumed " + consumed + " records waiting for datetime PK snapshots",
                15,
                record -> collectSnapshotId(idsByTable, record),
                true);

        assertTableSnapshot("dt_pk", idsByTable, DT_PK_ROW_COUNT);
        assertTableSnapshot("dt_max_003", idsByTable, DT_MAX_ROW_COUNT);
        assertTableSnapshot("dt_max_007", idsByTable, DT_MAX_ROW_COUNT);
    }

    private static boolean hasExpectedSnapshots(Map<String, List<Integer>> idsByTable) {
        return uniqueIds(idsByTable, "dt_pk").size() == DT_PK_ROW_COUNT
                && uniqueIds(idsByTable, "dt_max_003").size() == DT_MAX_ROW_COUNT
                && uniqueIds(idsByTable, "dt_max_007").size() == DT_MAX_ROW_COUNT;
    }

    private static void collectSnapshotId(Map<String, List<Integer>> idsByTable, SourceRecord record) {
        final String topic = record.topic();
        final String prefix = "server1.testDB1.dbo.";
        if (topic == null || !topic.startsWith(prefix) || !(record.value() instanceof Struct value)) {
            return;
        }
        final String table = topic.substring(prefix.length());
        if (!"dt_pk".equals(table) && !"dt_max_003".equals(table) && !"dt_max_007".equals(table)) {
            return;
        }
        final Struct after = value.getStruct("after");
        if (after == null || after.schema().field("id") == null) {
            return;
        }
        idsByTable.computeIfAbsent(table, key -> new ArrayList<>()).add(after.getInt32("id"));
    }

    private static void assertTableSnapshot(String table, Map<String, List<Integer>> idsByTable, int expectedRowCount) {
        final List<Integer> ids = idsByTable.getOrDefault(table, List.of());
        assertThat(ids)
                .as("%s emitted ids: %s", table, ids)
                .hasSize(expectedRowCount);
        assertThat(uniqueIds(idsByTable, table))
                .as("%s unique ids: %s", table, uniqueIds(idsByTable, table))
                .hasSize(expectedRowCount);
    }

    private static Set<Integer> uniqueIds(Map<String, List<Integer>> idsByTable, String table) {
        return new HashSet<>(idsByTable.getOrDefault(table, List.of()));
    }
}
