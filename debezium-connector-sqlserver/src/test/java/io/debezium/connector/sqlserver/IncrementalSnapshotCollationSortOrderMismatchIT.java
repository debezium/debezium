/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.AbstractSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * Tests for <a href="https://issues.redhat.com/browse/DBZ-7359">DBZ-7359</a>
 */
public class IncrementalSnapshotCollationSortOrderMismatchIT extends AbstractSnapshotTest<SqlServerConnector> {
    private static final int POLLING_INTERVAL = 1;
    // Sort order of SQL_ collation does not correspond with Unicode
    // see - https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support?view=sql-server-ver16#SQL-collations
    private static final String SQL_COLLATION = "SQL_Latin1_General_CP1_CI_AS";
    // IDs that simulate the sort order mismatch between the SQL_ collation and Unicode.
    private static final List<String> ALL_IDS = new ArrayList<>();
    // Contains the IDs that get skipped when there is a sort order mismatch.
    private static final List<String> SKIPPED_IDS = new ArrayList<>();

    private SqlServerConnection connection;
    private boolean isSendStringParametersAsUnicode;

    @BeforeClass
    public static void beforeClass() throws IOException {
        IoUtil.readLines("dbz-7359-ids.txt",
                IncrementalSnapshotCollationSortOrderMismatchIT.class.getClassLoader(),
                IncrementalSnapshotCollationSortOrderMismatchIT.class,
                ALL_IDS::add);
        // Of the IDS loaded, the 36 records with ids between Y-11-3-4 and Y1-01-1-1 exclusive, would be consistently skipped.
        SKIPPED_IDS.addAll(ALL_IDS.subList(ALL_IDS.indexOf("Y-11-3-4") + 1, ALL_IDS.indexOf("Y1-01-1-1")));
    }

    //
    // PK CHAR
    @Test
    public void orderMismatchPkCharValueIntParamsAsUnicodeFalse() throws Exception {
        orderMismatchPkTypecharValueInt(false, ALL_IDS.size(), "char(50) COLLATE " + SQL_COLLATION);
    }

    @Test
    public void orderMismatchPkCharValueIntParamsAsUnicodeTrueSkip36() throws Exception {
        orderMismatchPkTypecharValueInt(true, ALL_IDS.size() - SKIPPED_IDS.size(), "char(50) COLLATE " + SQL_COLLATION);
    }

    //
    // PK TEXT - Cannot be used as a PK column so not tested.

    //
    // PK VARCHAR
    @Test
    public void orderMismatchPkVarcharValueIntParamsAsUnicodeFalse() throws Exception {
        orderMismatchPkTypecharValueInt(false, ALL_IDS.size(), "varchar(50) COLLATE " + SQL_COLLATION);
    }

    @Test
    public void orderMismatchPkVarcharValueIntParamsAsUnicodeTrueSkip36() throws Exception {
        orderMismatchPkTypecharValueInt(true, ALL_IDS.size() - SKIPPED_IDS.size(), "varchar(50) COLLATE " + SQL_COLLATION);
    }

    // Ensure unicode values are read back ok
    @Test
    public void orderMismatchPkVarcharValueNvarcharParamsAsUnicodeFalse() throws Exception {
        orderMismatchPkVarcharValueNvarchar(false, ALL_IDS.size());
    }

    @Test
    public void orderMismatchPkVarcharValueNvarcharParamsAsUnicodeTrueSkip36() throws Exception {
        orderMismatchPkVarcharValueNvarchar(true, ALL_IDS.size() - SKIPPED_IDS.size());
    }

    //
    // PK - NCHAR
    @Test
    public void orderMismatchPkNcharValueNvarcharParamsAsUnicodeFalse() throws Exception {
        orderMismatchPkNtypeValueNvarchar(false, ALL_IDS.size(), "nchar(50)");
    }

    @Test
    public void orderMismatchPkNcharValueNvarcharParamsAsUnicodeTrue() throws Exception {
        orderMismatchPkNtypeValueNvarchar(true, ALL_IDS.size(), "nchar(50)");
    }

    //
    // PK - NTEXT - Cannot be used as a PK column so not tested.

    //
    // PK - NVARCHAR
    @Test
    public void orderMismatchPkNvarcharValueNvarcharParamsAsUnicodeFalse() throws Exception {
        orderMismatchPkNtypeValueNvarchar(false, ALL_IDS.size(), "nvarchar(50)");
    }

    @Test
    public void orderMismatchPkNvarcharValueNvarcharParamsAsUnicodeTrue() throws Exception {
        orderMismatchPkNtypeValueNvarchar(true, ALL_IDS.size(), "nvarchar(50)");
    }

    protected void orderMismatchPkTypecharValueInt(final boolean isSendStringParametersAsUnicode, int expectedRecordCount, String pkDataType) throws Exception {
        runTest(isSendStringParametersAsUnicode,
                expectedRecordCount,
                pkDataType,
                (pk, _idx) -> String.format("'%s'", pk),
                // NOTE: we use .trim() to handle the padding added when a non-variable length type is used.
                pk -> pk.getString(pkFieldName()).trim(),
                "int",
                (_pk, idx) -> String.format("%d", idx),
                record -> ((Struct) record.value()).getStruct("after").getInt32(valueFieldName()),
                dbChanges -> {
                    boolean result = true;
                    for (int i = 0; i < ALL_IDS.size(); i++) {
                        var id = ALL_IDS.get(i);
                        // NOTE: in the case where isSendStringParametersAsUnicode is true, the 36 records will
                        // have been skipped (i.e. you need to set it to false for it to work).
                        if (isSendStringParametersAsUnicode && SKIPPED_IDS.contains(id)) {
                            continue;
                        }
                        var val = dbChanges.get(id);
                        if (val == null || val != i) {
                            result = false;
                            Testing.printError(ALL_IDS.get(i) + " value is not = " + i + ", is = " + val);
                            break;
                        }
                    }
                    return result;
                });
    }

    protected void orderMismatchPkVarcharValueNvarchar(boolean isSendStringParametersAsUnicode, int expectedRecordCount) throws Exception {
        runTest(isSendStringParametersAsUnicode,
                expectedRecordCount,
                "varchar(50) COLLATE " + SQL_COLLATION,
                (pk, _idx) -> String.format("'%s'", pk),
                pk -> pk.getString(pkFieldName()),
                "nvarchar(100) not null",
                (_pk, idx) -> String.format("N'%d Hiragana: の, は, でした, Katakana: コンサート, Kanji: 昨夜, 最高'", idx),
                record -> ((Struct) record.value()).getStruct("after").getString(valueFieldName()),
                dbChanges -> {
                    boolean result = true;
                    for (int i = 0; i < ALL_IDS.size(); i++) {
                        var id = ALL_IDS.get(i);
                        // NOTE: in the case where isSendStringParametersAsUnicode is true, the 36 records will
                        // have been skipped (i.e. you need to set it to false for it to work).
                        if (isSendStringParametersAsUnicode && SKIPPED_IDS.contains(id)) {
                            continue;
                        }
                        var expectedVal = String.format("%d Hiragana: の, は, でした, Katakana: コンサート, Kanji: 昨夜, 最高", i);
                        var val = dbChanges.get(ALL_IDS.get(i));
                        if (!expectedVal.equals(val)) {
                            result = false;
                            Testing.printError(ALL_IDS.get(i) + " value is not = " + expectedVal + ", is = " + val);
                            break;
                        }
                    }
                    return result;
                });
    }

    protected void orderMismatchPkNtypeValueNvarchar(boolean isSendStringParametersAsUnicode, int expectedRecordCount, String pkDataType) throws Exception {
        runTest(isSendStringParametersAsUnicode,
                expectedRecordCount,
                pkDataType,
                (pk, _idx) -> String.format("N'の %s'", pk),
                // NOTE: we use .trim() to handle the padding added when a non-variable length type is used.
                pk -> pk.getString(pkFieldName()).trim(),
                "nvarchar(100) not null",
                (_pk, idx) -> String.format("N'%d Hiragana: の, は, でした, Katakana: コンサート, Kanji: 昨夜, 最高'", idx),
                record -> ((Struct) record.value()).getStruct("after").getString(valueFieldName()),
                dbChanges -> {
                    boolean result = true;
                    for (int i = 0; i < ALL_IDS.size(); i++) {
                        var expectedVal = String.format("%d Hiragana: の, は, でした, Katakana: コンサート, Kanji: 昨夜, 最高", i);
                        var val = dbChanges.get(String.format("の %s", ALL_IDS.get(i)));
                        if (!expectedVal.equals(val)) {
                            result = false;
                            System.err.println(ALL_IDS.get(i) + " value is not = " + expectedVal + ", is = " + val);
                            break;
                        }
                    }
                    return result;
                });
    }

    protected <P, V> void runTest(boolean isSendStringParametersAsUnicode,
                                  int expectedRecordCount,
                                  String pkDataType,
                                  BiFunction<String, Integer, String> pkInsValFn,
                                  Function<Struct, P> pkConverter,
                                  String valueDataType,
                                  BiFunction<String, Integer, String> valueInsValFn,
                                  Function<SourceRecord, V> valueConverter,
                                  Predicate<Map<P, V>> validateDbChanges)
            throws Exception {
        // NOTE: this value is referenced in the config() method below to ensure the property is
        // configured properly when startConnector() is called.
        this.isSendStringParametersAsUnicode = isSendStringParametersAsUnicode;
        TestHelper.createTestDatabase();
        try (SqlServerConnection connection = TestHelper.testConnection(TestHelper.TEST_DATABASE_1)) {
            this.connection = connection;

            connection.execute(
                    String.format("CREATE TABLE %s (%s %s primary key, %s %s)",
                            tableName(), pkFieldName(), pkDataType, valueFieldName(), valueDataType),
                    "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
            TestHelper.enableTableCdc(connection, "debezium_signal");
            TestHelper.adjustCdcPollingInterval(connection, POLLING_INTERVAL);

            initializeConnectorTestFramework();
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

            populateTable(connection, pkInsValFn, valueInsValFn);

            TestHelper.enableTableCdc(connection, tableName());

            startConnector();

            sendAdHocSnapshotSignal(tableName());

            testIncrementalSnapshotConsumed(expectedRecordCount, pkConverter, valueConverter, validateDbChanges);
        }
    }

    protected void populateTable(SqlServerConnection connection,
                                 BiFunction<String, Integer, String> pkInsValFn,
                                 BiFunction<String, Integer, String> valueInsValFn)
            throws SQLException {
        connection.setAutoCommit(false);
        for (int i = 0; i < ALL_IDS.size(); i++) {
            var id = ALL_IDS.get(i);
            connection.executeWithoutCommitting(
                    String.format("INSERT INTO %s (%s, %s) VALUES(%s, %s)",
                            tableName(), pkFieldName(), valueFieldName(),
                            pkInsValFn.apply(id, i), valueInsValFn.apply(id, i)));
        }
        connection.commit();
    }

    protected <P, V> void testIncrementalSnapshotConsumed(int expectedRecordCount,
                                                          Function<Struct, P> pkConverter,
                                                          Function<SourceRecord, V> valueConverter,
                                                          Predicate<Map<P, V>> validateDbChanges)
            throws InterruptedException {
        final Map<P, V> dbChanges = consumeIncrementalSnapshot(
                // recordCount
                expectedRecordCount,
                // dataCompleted
                x -> true,
                // pkConverter
                pkConverter,
                // valueConverter
                valueConverter,
                // topicName
                topicName(),
                // recordConsumer
                null,
                true);

        assertThat(dbChanges).hasSize(expectedRecordCount);
        Assert.assertTrue(validateDbChanges.test(dbChanges));
    }

    protected <P, V> Map<P, V> consumeIncrementalSnapshot(int recordCount,
                                                          Predicate<Map.Entry<P, V>> dataCompleted,
                                                          Function<Struct, P> pkConverter,
                                                          Function<SourceRecord, V> valueConverter,
                                                          String topicName,
                                                          Consumer<List<SourceRecord>> recordConsumer,
                                                          boolean assertRecords)
            throws InterruptedException {
        final Map<P, V> dbChanges = new HashMap<>();
        int noRecords = 0;
        for (;;) {
            final SourceRecords records = consumeRecordsByTopic(1, assertRecords);
            final List<SourceRecord> dataRecords = records.recordsForTopic(topicName);
            if (records.allRecordsInOrder().isEmpty()) {
                noRecords++;
                assertThat(noRecords).describedAs(String.format("Too many no data record results, %d < %d", dbChanges.size(), recordCount))
                        .isLessThanOrEqualTo(5);
                continue;
            }
            noRecords = 0;
            if (dataRecords == null || dataRecords.isEmpty()) {
                continue;
            }
            dataRecords.forEach(record -> {
                final P id = pkConverter.apply((Struct) record.key());
                final V value = valueConverter.apply(record);
                dbChanges.put(id, value);
            });
            if (recordConsumer != null) {
                recordConsumer.accept(dataRecords);
            }
            if (dbChanges.size() >= recordCount) {
                if (dbChanges.entrySet().stream().noneMatch(dataCompleted.negate())) {
                    break;
                }
            }
        }

        return dbChanges;
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
        return "server1." + TestHelper.TEST_DATABASE_1 + ".dbo.c";
    }

    @Override
    protected String tableName() {
        return TestHelper.TEST_DATABASE_1 + ".dbo.c";
    }

    @Override
    protected List<String> topicNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> tableNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String signalTableName() {
        return "dbo.debezium_signal";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(ConfigurationNames.DATABASE_CONFIG_PREFIX + "sendStringParametersAsUnicode", isSendStringParametersAsUnicode)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.TEST_DATABASE_1 + ".dbo.debezium_signal")
                .with(SqlServerConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 250);
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
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }
}
