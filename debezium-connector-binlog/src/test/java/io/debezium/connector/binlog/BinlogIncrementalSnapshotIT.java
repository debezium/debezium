/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotWithSchemaChangesSupportTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;

public abstract class BinlogIncrementalSnapshotIT<C extends SourceConnector>
        extends AbstractIncrementalSnapshotWithSchemaChangesSupportTest<C>
        implements BinlogConnectorTest<C> {

    protected static final String SERVER_NAME = "is_test";
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "incremental_snapshot-test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @Before
    public void before() throws SQLException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Override
    protected Class<C> connectorClass() {
        return getConnectorClass();
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return getTestDatabaseConnection(DATABASE.getDatabaseName());
    }

    @Override
    protected String connector() {
        return getConnectorName();
    }

    protected abstract Class<?> getFieldReader();

    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(BinlogConnectorConfig.USER, "mysqluser")
                .with(BinlogConnectorConfig.PASSWORD, "mysqlpw")
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName("debezium_signal"))
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 1)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, String.format("%s:%s", DATABASE.qualifiedTableName("a42"), "pk1,pk2,pk3,pk4"))
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, SchemaNameAdjustmentMode.AVRO);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = DATABASE.qualifiedTableName("c");
        }
        else {
            tableIncludeList = DATABASE.qualifiedTableName("a") + ", " + DATABASE.qualifiedTableName("c");
        }
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SQL_QUERY, true)
                .with(BinlogConnectorConfig.USER, "mysqluser")
                .with(BinlogConnectorConfig.PASSWORD, "mysqlpw")
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.SIGNAL_DATA_COLLECTION, DATABASE.qualifiedTableName("debezium_signal"))
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, String.format("%s:%s", DATABASE.qualifiedTableName("a42"), "pk1,pk2,pk3,pk4"))
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(BinlogConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl)
                .with(CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, SchemaNameAdjustmentMode.AVRO);
    }

    @Override
    protected String server() {
        return DATABASE.getServerName();
    }

    @Override
    protected String topicName() {
        return DATABASE.topicForTable("a");
    }

    @Override
    protected List<String> topicNames() {
        return List.of(DATABASE.topicForTable("a"), DATABASE.topicForTable("c"));
    }

    @Override
    protected String tableName() {
        return tableNameId().toQuotedString('`');
    }

    @Override
    protected String noPKTopicName() {
        return DATABASE.topicForTable("a42");
    }

    @Override
    protected String noPKTableName() {
        return tableNameId("a42").toQuotedString('`');
    }

    @Override
    protected List<String> tableNames() {
        final String tableA = TableId.parse(DATABASE.qualifiedTableName("a")).toQuotedString('`');
        final String tableB = TableId.parse(DATABASE.qualifiedTableName("c")).toQuotedString('`');
        return List.of(tableA, tableB);
    }

    @Override
    protected String signalTableName() {
        return tableNameId("debezium_signal").toQuotedString('`');
    }

    @Override
    protected String signalTableNameSanitized() {
        return DATABASE.qualifiedTableName("debezium_signal");
    }

    @Override
    protected String tableName(String table) {
        return tableNameId(table).toQuotedString('`');
    }

    @Override
    protected String tableDataCollectionId() {
        return tableNameId().toString();
    }

    @Override
    protected String noPKTableDataCollectionId() {
        return tableNameId("a42").toString();
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return List.of(tableNameId().toString(), tableNameId("c").toString());
    }

    private String dataCollectionName(String table) {
        return tableNameId(table).toString();
    }

    private TableId tableNameId() {
        return tableNameId("a");
    }

    private TableId tableNameId(String table) {
        return TableId.parse(DATABASE.qualifiedTableName(table));
    }

    @Override
    protected String alterColumnStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s", table, column, type);
    }

    @Override
    protected String alterColumnSetNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s NOT NULL", table, column, type);
    }

    @Override
    protected String alterColumnDropNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s NULL", table, column, type);
    }

    @Override
    protected String alterColumnSetDefaultStatement(String table, String column, String type, String defaultValue) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s DEFAULT %s", table, column, type, defaultValue);
    }

    @Override
    protected String alterColumnDropDefaultStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s MODIFY COLUMN %s %s", table, column, type);
    }

    @Override
    protected void executeRenameTable(JdbcConnection connection, String newTable) throws SQLException {
        connection.setAutoCommit(false);
        String query = String.format("RENAME TABLE %s to %s, %s to %s", tableName(), "old_table", newTable, tableName());
        logger.info(query);
        connection.executeWithoutCommitting(query);
        connection.commit();
    }

    @Override
    protected String createTableStatement(String newTable, String copyTable) {
        return String.format("CREATE TABLE %s LIKE %s", newTable, copyTable);
    }

    @Test
    public void updates() throws Exception {
        // Testing.Print.enable();

        populateTable();
        startConnector();

        sendAdHocSnapshotSignal();

        final int batchSize = 10;
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            ((BinlogTestConnection) connection).setBinlogRowQueryEventsOn();
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(
                        String.format("UPDATE %s SET aa = aa + 2000 WHERE pk > %s AND pk <= %s", tableName(),
                                i * batchSize, (i + 1) * batchSize));
                connection.commit();
            }
        }

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, SourceRecord> dbChanges = consumeRecordsMixedWithIncrementalSnapshot(expectedRecordCount,
                x -> ((Struct) x.getValue().value()).getStruct("after").getInt32(valueFieldName()) >= 2000, null);
        for (int i = 0; i < expectedRecordCount; i++) {
            SourceRecord record = dbChanges.get(i + 1);
            final int value = ((Struct) record.value()).getStruct("after").getInt32(valueFieldName());
            assertEquals(i + 2000, value);
            Object query = ((Struct) record.value()).getStruct("source").get("query");
            String snapshot = ((Struct) record.value()).getStruct("source").get("snapshot").toString();
            if (snapshot.equals("false")) {
                assertNotNull(query);
            }
            else {
                assertNull(query);
                assertEquals("incremental", snapshot);
            }
        }
    }

    @Test
    @FixFor("DBZ-4939")
    public void tableWithDatetime() throws Exception {
        // Testing.Print.enable();
        final int ROWS = 10;

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROWS; i++) {
                connection.executeWithoutCommitting(String.format(
                        "INSERT INTO a_dt (pk, dt, d, t) VALUES (%s, TIMESTAMP('%s-05-01'), '%s-05-01', '%s:00:00')",
                        i + 1, i + 2000, i + 2000, i));
            }
            connection.commit();
        }

        final Configuration config = config().with(BinlogConnectorConfig.SNAPSHOT_FETCH_SIZE, 5).build();
        start(connectorClass(), config, loggingCompletion());
        waitForConnectorToStart();
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        sendAdHocSnapshotSignal(dataCollectionName("a_dt"));

        final int expectedRecordCount = ROWS;
        final Map<Integer, List<Object>> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32(pkFieldName()),
                record -> {
                    long ts = ((Struct) record.value()).getStruct("after").getInt64("dt");
                    long tsSeconds = ts / 1000;
                    long tsMillis = ts % 1000;
                    LocalDateTime tsDateTime = LocalDateTime.ofEpochSecond(
                            tsSeconds,
                            (int) TimeUnit.MILLISECONDS.toNanos(tsMillis),
                            ZoneOffset.UTC);
                    int dateTs = ((Struct) record.value()).getStruct("after").getInt32("d");
                    LocalDate date = LocalDate.ofEpochDay(dateTs);
                    long timeTs = ((Struct) record.value()).getStruct("after").getInt64("t");
                    LocalTime time = LocalTime.ofSecondOfDay(timeTs / 1_000_000);
                    return List.of(tsDateTime.toLocalDate(), date, time);
                },
                DATABASE.topicForTable("a_dt"),
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            LocalDateTime dateTime = LocalDateTime.parse(String.format("%s-05-01T00:00:00", 2000 + i));
            LocalDate dt = dateTime.toLocalDate();
            LocalDate d = LocalDate.parse(String.format("%s-05-01", 2000 + i));
            LocalTime t = LocalTime.parse(String.format("0%s:00:00", i));
            assertThat(dbChanges).contains(entry(i + 1, List.of(dt, d, t)));
        }
    }

    @Test
    @FixFor("DBZ-5099")
    public void tableWithZeroDate() throws Exception {
        // Testing.Print.enable();
        final LogInterceptor logInterceptor = new LogInterceptor(getFieldReader());

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            connection.executeWithoutCommitting("INSERT INTO a_date (pk) VALUES (1)");
            connection.commit();
        }

        final Configuration config = config().with(BinlogConnectorConfig.SNAPSHOT_FETCH_SIZE, 5).build();
        start(connectorClass(), config, loggingCompletion());
        waitForConnectorToStart();
        waitForAvailableRecords(5, TimeUnit.SECONDS);

        sendAdHocSnapshotSignal(dataCollectionName("a_date"));

        final int expectedRecordCount = 1;
        final Map<Integer, List<Integer>> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32(pkFieldName()),
                record -> {
                    Integer d = (((Struct) record.value()).getStruct("after").getInt32("d"));
                    Integer d_opt = (((Struct) record.value()).getStruct("after").getInt32("d_opt"));
                    return Arrays.asList(d, d_opt);
                },
                DATABASE.topicForTable("a_date"),
                null);
        assertThat(dbChanges).contains(entry(1, Arrays.asList(0, null)));
        assertFalse(logInterceptor.containsWarnMessage("Invalid length when read MySQL DATE value. BIN_LEN_DATE is 0."));
    }

    @Test
    @FixFor("DBZ-6937")
    public void incrementalSnapshotOnly() throws Exception {
        // Testing.Print.enable();

        populateTable();
        final Configuration config = config().with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER).build();
        start(connectorClass(), config, loggingCompletion());

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }
}
