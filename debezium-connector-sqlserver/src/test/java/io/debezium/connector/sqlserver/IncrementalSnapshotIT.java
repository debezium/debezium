/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.Flaky;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotWithSchemaChangesSupportTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotWithSchemaChangesSupportTest<SqlServerConnector> {
    private static final int POLLING_INTERVAL = 1;

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE a (pk int primary key, aa int)",
                "CREATE TABLE b (pk int primary key, aa int)",
                "CREATE TABLE a42 (pk1 integer, pk2 integer, pk3 integer, pk4 integer, aa integer);",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        TestHelper.enableTableCdc(connection, "debezium_signal");
        TestHelper.adjustCdcPollingInterval(connection, POLLING_INTERVAL);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        // In some cases the max lsn from lsn_time_mapping table was coming out to be null, since
        // the operations done above needed some time to be captured by the capture process.
        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis() * 2);
    }

    @AfterEach
    void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected void populateTable() throws SQLException {
        super.populateTable();
        TestHelper.enableTableCdc(connection, "a");
    }

    @Override
    protected void populateTables() throws SQLException {
        super.populateTables();
        TestHelper.enableTableCdc(connection, "a");
        TestHelper.enableTableCdc(connection, "b");
    }

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "server1.testDB1.dbo.a";
    }

    @Override
    protected List<String> topicNames() {
        return List.of("server1.testDB1.dbo.a", "server1.testDB1.dbo.b");
    }

    @Override
    protected String tableName() {
        return "testDB1.dbo.a";
    }

    @Override
    protected String noPKTopicName() {
        return "server1.testDB1.dbo.a42";
    }

    @Override
    protected String noPKTableName() {
        return "testDB1.dbo.a42";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("testDB1.dbo.a", "testDB1.dbo.b");
    }

    @Override
    protected String tableName(String table) {
        return "testDB1.dbo." + table;
    }

    @Override
    protected String signalTableName() {
        return "dbo.debezium_signal";
    }

    @Override
    protected String signalExcludingColumnIncludeList() {
        return "dbo.a.pk,dbo.a.aa";
    }

    @Override
    protected String alterColumnStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s", table, column, type);
    }

    @Override
    protected String alterColumnSetNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s NOT NULL", table, column, type);
    }

    @Override
    protected String alterColumnDropNotNullStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s NULL", table, column, type);
    }

    @Override
    protected String alterColumnSetDefaultStatement(String table, String column, String type, String defaultValue) {
        return String.format("ALTER TABLE %s ADD CONSTRAINT df_%s DEFAULT %s FOR %s", table, column, defaultValue, column);
    }

    @Override
    protected String alterColumnDropDefaultStatement(String table, String column, String type) {
        return String.format("ALTER TABLE %s DROP CONSTRAINT df_%s", table, column);
    }

    @Override
    protected void executeRenameTable(JdbcConnection connection, String newTable) throws SQLException {
        TestHelper.disableTableCdc(connection, "a");
        connection.setAutoCommit(false);
        logger.info(String.format("exec sp_rename '%s', '%s'", tableName(), "old_table"));
        connection.executeWithoutCommitting(String.format("exec sp_rename '%s', '%s'", tableName(), "old_table"));
        logger.info(String.format("exec sp_rename '%s', '%s'", tableName(newTable), "a"));
        connection.executeWithoutCommitting(String.format("exec sp_rename '%s', '%s'", tableName(newTable), "a"));
        TestHelper.enableTableCdc(connection, "a", "a", Arrays.asList("pk", "aa", "c"));
        connection.commit();
    }

    @Override
    protected String createTableStatement(String newTable, String copyTable) {
        return String.format("CREATE TABLE %s (pk int primary key, aa int)", newTable);
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250)
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "dbo.a42:pk1,pk2,pk3,pk4");
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "dbo.b";
        }
        else {
            tableIncludeList = "dbo.a,dbo.b";
        }
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB1.dbo.debezium_signal")
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250)
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES, true)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "dbo.a42:pk1,pk2,pk3,pk4")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected void waitForCdcTransactionPropagation(int expectedTransactions) throws Exception {
        TestHelper.waitForCdcTransactionPropagation(connection, TestHelper.TEST_DATABASE_1, expectedTransactions);
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String database() {
        return TestHelper.TEST_DATABASE_1;
    }

    @Override
    @Flaky("DBZ-5393")
    public void stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart() throws Exception {
        super.stopCurrentIncrementalSnapshotWithAllCollectionsAndTakeNewNewIncrementalSnapshotAfterRestart();
    }

    @Test
    @FixFor("debezium/dbz#488")
    public void snapshotWithExcludedKeyColumn() throws Exception {
        populateTable();
        startConnector(x -> x.with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.a.pk"));

        sendAdHocSnapshotSignal();

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(expectedRecordCount);
        for (int i = 0; i < expectedRecordCount; i++) {
            assertThat(dbChanges).contains(entry(i + 1, i));
        }
    }

    @Test
    public void snapshotWithMultipleDatabases() throws Exception {
        // Testing.Print.enable();

        TestHelper.createTestDatabase(TestHelper.TEST_DATABASE_2);

        try (SqlServerConnection connection2 = TestHelper.testConnection(TestHelper.TEST_DATABASE_2)) {

            connection2.execute(
                    "CREATE TABLE b (pk int primary key, bb int)",
                    "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
            TestHelper.enableTableCdc(connection2, "b");
            TestHelper.enableTableCdc(connection2, "debezium_signal");
            TestHelper.adjustCdcPollingInterval(connection2, POLLING_INTERVAL);

            for (int i = 1; i <= 10; i++) {
                connection.execute(String.format("INSERT INTO a VALUES (%d, %d)", i, i * 10));
                connection2.execute(String.format("INSERT INTO b VALUES (%d, %d)", i, i * 100));
            }

            waitForInsertsToBeCaptured(connection2);

            final Configuration config = TestHelper.defaultConnectorConfig()
                    .with(SqlServerConnectorConfig.DATABASE_NAMES, TestHelper.TEST_DATABASE_1 + "," + TestHelper.TEST_DATABASE_2)
                    .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION,
                            String.join(",", TestHelper.TEST_DATABASE_1 + ".dbo.debezium_signal",
                                    TestHelper.TEST_DATABASE_2 + ".dbo.debezium_signal"))
                    .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 5)
                    .build();

            start(SqlServerConnector.class, config);
            assertConnectorIsRunning();

            TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_1);
            TestHelper.waitForDatabaseSnapshotToBeCompleted(TestHelper.TEST_DATABASE_2);
            TestHelper.waitForStreamingStarted();

            connection.execute(String.format(
                    "INSERT INTO debezium_signal VALUES ('signal-1', 'execute-snapshot', " +
                            "'{\"data-collections\": [\"%s.dbo.a\"]}')",
                    TestHelper.TEST_DATABASE_1));

            final int expectedRecordsCount = 10;
            final Map<Integer, SourceRecord> tableARecords = consumeIncrementalSnapshotReads(expectedRecordsCount,
                    "server1." + TestHelper.TEST_DATABASE_1 + ".dbo.a");

            for (int pk = 1; pk <= expectedRecordsCount; pk++) {
                final SourceRecord record = tableARecords.get(pk);
                assertThat(record).isNotNull();
                final Struct value = (Struct) record.value();
                assertThat(value.getStruct("after").getInt32("pk")).isEqualTo(pk);
                assertThat(value.getStruct("after").getInt32("aa")).isEqualTo(pk * 10);
                assertThat(record.sourcePartition().get("server")).isEqualTo("server1");
                assertThat(record.sourcePartition().get("database")).isEqualTo(TestHelper.TEST_DATABASE_1);
            }

            connection2.execute(String.format(
                    "INSERT INTO debezium_signal VALUES ('signal-2', 'execute-snapshot', " +
                            "'{\"data-collections\": [\"%s.dbo.b\"]}')",
                    TestHelper.TEST_DATABASE_2));

            final Map<Integer, SourceRecord> tableBRecords = consumeIncrementalSnapshotReads(expectedRecordsCount,
                    "server1." + TestHelper.TEST_DATABASE_2 + ".dbo.b");

            for (int pk = 1; pk <= expectedRecordsCount; pk++) {
                final SourceRecord record = tableBRecords.get(pk);
                assertThat(record).isNotNull();
                final Struct value = (Struct) record.value();
                assertThat(value.getStruct("after").getInt32("pk")).isEqualTo(pk);
                assertThat(value.getStruct("after").getInt32("bb")).isEqualTo(pk * 100);
                assertThat(record.sourcePartition().get("server")).isEqualTo("server1");
                assertThat(record.sourcePartition().get("database")).isEqualTo(TestHelper.TEST_DATABASE_2);
            }
        }
    }

    @Test
    @DisplayName("When multiple databases are configured and only one single data collection, a log info should be emitted.")
    public void onSingleDataCollectionForMultipleDatabase() throws Exception {
        // Testing.Print.enable();

        LogInterceptor logInterceptor = new LogInterceptor(CommonConnectorConfig.class);

        TestHelper.createTestDatabase(TestHelper.TEST_DATABASE_2);

        try (SqlServerConnection connection2 = TestHelper.testConnection(TestHelper.TEST_DATABASE_2)) {

            connection2.execute(
                    "CREATE TABLE b (pk int primary key, bb int)",
                    "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
            TestHelper.enableTableCdc(connection2, "b");
            TestHelper.enableTableCdc(connection2, "debezium_signal");
            TestHelper.adjustCdcPollingInterval(connection2, POLLING_INTERVAL);

            for (int i = 1; i <= 10; i++) {
                connection.execute(String.format("INSERT INTO a VALUES (%d, %d)", i, i * 10));
                connection2.execute(String.format("INSERT INTO b VALUES (%d, %d)", i, i * 100));
            }

            waitForInsertsToBeCaptured(connection2);

            final Configuration config = TestHelper.defaultConnectorConfig()
                    .with(SqlServerConnectorConfig.DATABASE_NAMES, TestHelper.TEST_DATABASE_1 + "," + TestHelper.TEST_DATABASE_2)
                    .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION,
                            String.join(",", TestHelper.TEST_DATABASE_1 + ".dbo.debezium_signal"))
                    .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 5)
                    .build();

            start(SqlServerConnector.class, config);
            assertConnectorIsRunning();

            assertThat(logInterceptor.containsMessage(
                    "You set a single data collection for a multi task connector. If you want to send signals for each database you need to provide a signal data collection per database."));
        }
    }

    /**
     * Waits until the rows inserted before the connector starts are covered by the max LSN of both databases.
     * Otherwise the streaming start position can fall in the middle of the inserts, and the remaining rows
     * are streamed as create events.
     */
    private void waitForInsertsToBeCaptured(SqlServerConnection connection2) throws Exception {
        // Table testDB1.dbo.a has no change table, so only the max LSN can be checked.
        TestHelper.waitForMaxLsnAvailable(connection, TestHelper.TEST_DATABASE_1);
        TestHelper.waitForCdcRecordInDatabase(connection2, TestHelper.TEST_DATABASE_2, "b", rs -> rs.getInt("pk") == 10);
    }

    /**
     * Consumes records until the given number of distinct incremental snapshot reads arrived on the topic.
     * Records on other topics are skipped, and an entry only counts once its latest record is a read.
     */
    private Map<Integer, SourceRecord> consumeIncrementalSnapshotReads(int recordCount, String topicName) throws InterruptedException {
        return consumeMixedWithIncrementalSnapshot(recordCount,
                entry -> Envelope.Operation.READ.code().equals(((Struct) entry.getValue().value()).getString(Envelope.FieldName.OPERATION)),
                key -> key.getInt32("pk"),
                Function.identity(),
                topicName,
                null);
    }
}
