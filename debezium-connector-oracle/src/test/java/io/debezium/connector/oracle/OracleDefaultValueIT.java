/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.util.Testing;

/**
 * Integration test that tests all supported data types and default value combinations during
 * both snapshot and streaming phases in conjunction with schema changes to the table.
 *
 * @author Chris Cranford
 */
public class OracleDefaultValueIT extends AbstractConnectorTest {

    private OracleConnection connection;
    private Consumer<Configuration.Builder> configUpdater;
    private Configuration config;

    @Before
    public void before() throws Exception {
        configUpdater = builder -> {
        };
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        TestHelper.dropTable(connection, "default_value_test");
        TestHelper.dropSequence(connection, "debezium_seq");

        connection.execute("CREATE SEQUENCE debezium_seq MINVALUE 1 MAXVALUE 999999999 INCREMENT BY 1 START WITH 1");
    }

    @After
    public void after() throws Exception {
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "default_value_test");
            TestHelper.dropSequence(connection, "debezium_seq");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-3710")
    public void shouldHandleNumericDefaultTypes() throws Exception {
        // TODO: remove once we upgrade Apicurio version (DBZ-7357)
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_int", "int",
                        "1", "2",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_integer", "integer",
                        "1", "2",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_smallint", "smallint",
                        "1", "2",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_num", "number",
                        "1", "2",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_NO_DEFAULT),
                new ColumnDefinition("val_number_38_no_scale", "number(38)",
                        "1", "2",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_38_scale_0", "number(38,0)",
                        "1", "2",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_2", "number(2)",
                        "1", "2",
                        (byte) 1, (byte) 2,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_4", "number(4)",
                        "1", "2",
                        (short) 1, (short) 2,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_9", "number(9)",
                        "1", "2",
                        1, 2,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_18", "number(18)",
                        "1", "2",
                        1L, 2L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_2_neg_scale", "number(1,-1)",
                        "10", "20",
                        (byte) 10, (byte) 20,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_4_neg_scale", "number(2,-2)",
                        "100", "200",
                        (short) 100, (short) 200,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_9_neg_scale", "number(8,-1)",
                        "10", "20",
                        10, 20,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_918_neg_scale", "number(16,-2)",
                        "100", "200",
                        100L, 200L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_decimal", "decimal(10)",
                        "125", "250",
                        125L, 250L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_numeric", "numeric(10)",
                        "125", "250",
                        125L, 250L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_1", "number(1)",
                        "1", "2",
                        (byte) 1, (byte) 2,
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-3710")
    public void shouldHandleFloatPointDefaultTypes() throws Exception {
        // TODO: remove once we upgrade Apicurio version (DBZ-7357)
        if (VerifyRecord.isApucurioAvailable()) {
            skipAvroValidation();
        }

        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_bf", "binary_float",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_bd", "binary_double",
                        "3.14", "6.28",
                        3.14d, 6.28d,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_float", "float",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_NO_DEFAULT),
                new ColumnDefinition("val_float_10", "float(10)",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_NO_DEFAULT),
                new ColumnDefinition("val_double_precision", "double precision",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_NO_DEFAULT),
                new ColumnDefinition("val_real", "real",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_NO_DEFAULT),
                new ColumnDefinition("val_number_10_6", "number(10,6)",
                        "123.45", "234.57",
                        BigDecimal.valueOf(123450000, 6), BigDecimal.valueOf(234570000, 6),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_decimal_10_6", "decimal(10,6)",
                        "3.14", "6.28",
                        BigDecimal.valueOf(3140000, 6), BigDecimal.valueOf(6280000, 6),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_numeric_10_6", "numeric(10,6)",
                        "3.14", "6.28",
                        BigDecimal.valueOf(3140000, 6), BigDecimal.valueOf(6280000, 6),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_number_vs", "number",
                        "3.14", "6.28",
                        3.14f, 6.28f,
                        AssertionType.FIELD_NO_DEFAULT));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-3710")
    public void shouldHandleCharacterDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_varchar", "varchar(100)",
                        "'hello'", "'world'",
                        "hello", "world",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_varchar_paren", "varchar(100)",
                        "('hello')", "('world')",
                        "hello", "world",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_varchar2", "varchar2(100)",
                        "'red'", "'green'",
                        "red", "green",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_varchar2_paren", "varchar2(100)",
                        "('red')", "('green')",
                        "red", "green",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nvarchar2", "nvarchar2(100)",
                        "'cedric'", "'entertainer'",
                        "cedric", "entertainer",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nvarchar2_paren", "nvarchar2(100)",
                        "('cedric')", "('entertainer')",
                        "cedric", "entertainer",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_char", "char(5)",
                        "'YES'", "'NO'",
                        "YES  ", "NO   ",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_char_paren", "char(5)",
                        "('YES')", "('NO')",
                        "YES  ", "NO   ",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nchar", "nchar(5)",
                        "'ON'", "'OFF'",
                        "ON   ", "OFF  ",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_nchar_paren", "nchar(5)",
                        "('ON')", "('OFF')",
                        "ON   ", "OFF  ",
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-3710")
    public void shouldHandleDateTimeDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_date", "date",
                        "TO_DATE('2001-02-03 00:00:00', 'YYYY-MM-DD HH24:MI:SS')",
                        "TO_DATE('2005-01-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS')",
                        981158400000L, 1107129600000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_ts", "timestamp",
                        "TO_DATE('2001-02-03 01:02:03', 'YYYY-MM-DD HH24:MI:SS')",
                        "TO_DATE('2005-01-31 02:03:04', 'YYYY-MM-DD HH24:MI:SS')",
                        981162123000000L, 1107136984000000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_ts_prec2", "timestamp(2)",
                        "TO_DATE('2001-02-03 01:02:03', 'YYYY-MM-DD HH24:MI:SS')",
                        "TO_DATE('2005-01-31 02:03:04', 'YYYY-MM-DD HH24:MI:SS')",
                        981162123000L, 1107136984000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_ts_prec4", "timestamp(4)",
                        "TO_DATE('2001-02-03 01:02:03', 'YYYY-MM-DD HH24:MI:SS')",
                        "TO_DATE('2005-01-31 02:03:04', 'YYYY-MM-DD HH24:MI:SS')",
                        981162123000000L, 1107136984000000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_ts_prec9", "timestamp(9)",
                        "TO_DATE('2001-02-03 01:02:03', 'YYYY-MM-DD HH24:MI:SS')",
                        "TO_DATE('2005-01-31 02:03:04', 'YYYY-MM-DD HH24:MI:SS')",
                        981162123000000000L, 1107136984000000000L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_tstz", "timestamp with time zone",
                        "TO_TIMESTAMP_TZ('2018-03-27 01:34:56.00789 -11:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM')",
                        "TO_TIMESTAMP_TZ('2019-04-28 02:35:57.00891 -10:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM')",
                        "2018-03-27T01:34:56.007890-11:00",
                        "2019-04-28T02:35:57.008910-10:00",
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_tsltz", "timestamp with local time zone",
                        "TO_TIMESTAMP_TZ('2018-03-27 01:34:56.00789 -11:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM')",
                        "TO_TIMESTAMP_TZ('2019-04-28 02:35:57.00891 -10:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM')",
                        "2018-03-27T12:34:56.007890Z", // 1am + 11 hours, stored in UTC and returned in UTC
                        "2019-04-28T12:35:57.008910Z", // 2am + 10 hours, stored in UTC and returned in UTC
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-3710")
    public void shouldHandleIntervalDefaultTypes() throws Exception {
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_int_ytm", "interval year to month",
                        "'5-3'", "'7-4'",
                        getOracleIntervalYearMonth(5, 3), getOracleIntervalYearMonth(7, 4),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_int_dts", "interval day(3) to second(3)",
                        "'5 1:2:3.456'", "'3 2:1:4.567'",
                        getOracleIntervalDaySecond(5, 1, 2, 3, 456000), getOracleIntervalDaySecond(3, 2, 1, 4, 567000),
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-1539")
    public void shouldHandleIntervalDefaultTypesAsString() throws Exception {
        configUpdater = builder -> {
            builder.with(OracleConnectorConfig.INTERVAL_HANDLING_MODE,
                    OracleConnectorConfig.IntervalHandlingMode.STRING.getValue());
        };
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_int_ytm", "interval year to month",
                        "'5-3'", "'7-4'",
                        getOracleIntervalYearMonthString(5, 3),
                        getOracleIntervalYearMonthString(7, 4),
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_int_dts", "interval day(3) to second(3)",
                        "'5 1:2:3.456'", "'3 2:1:4.567'",
                        getOracleIntervalDaySecondString(5, 1, 2, 3, 456000),
                        getOracleIntervalDaySecondString(3, 2, 1, 4, 567000),
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
    }

    @Test
    @FixFor("DBZ-4208")
    public void shouldHandleDefaultValueFromSequencesAsNoDefault() throws Exception {
        // Used to track the number of default value parser exceptions
        LogInterceptor logInterceptor = new LogInterceptor(OracleDefaultValueConverter.class);

        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_id", "number(18,0)",
                        "debezium_seq.nextval", "debezium_seq.nextval",
                        BigDecimal.valueOf(1L), BigDecimal.valueOf(2L),
                        AssertionType.FIELD_NO_DEFAULT));

        shouldHandleDefaultValuesCommon(columnDefinitions);
        assertThat(logInterceptor.countOccurrences("Cannot parse column default value")).isEqualTo(6);
    }

    @Test
    @FixFor("DBZ-4360")
    public void shouldHandleDefaultValuesWhereSqlMayContainsTrailingSpaces() throws Exception {
        LogInterceptor logInterceptor = new LogInterceptor(OracleDefaultValueConverter.class);
        List<ColumnDefinition> columnDefinitions = Arrays.asList(
                new ColumnDefinition("val_num", "number(15)",
                        "null ", "null ",
                        null, null,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_num2", "number(15)",
                        "2 ", "3 ",
                        2L, 3L,
                        AssertionType.FIELD_DEFAULT_EQUAL),
                new ColumnDefinition("val_char", "char(3)",
                        "'No' ", "'NO' ",
                        "No ", "NO ",
                        AssertionType.FIELD_DEFAULT_EQUAL));

        shouldHandleDefaultValuesCommon(columnDefinitions);
        assertThat(logInterceptor.countOccurrences("Cannot parse column default value")).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-4388")
    public void shouldHandleDefaultValueForNonOptionalColumnUsingUnparseableValues() throws Exception {
        TestHelper.dropTable(connection, "dbz4388");
        try {
            connection.execute("CREATE TABLE dbz4388 (" +
                    "  id NUMBER(9) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY," +
                    "  first_name VARCHAR2(255) NOT NULL," +
                    "  last_name VARCHAR2(255) NOT NULL," +
                    "  email VARCHAR2(255) NOT NULL UNIQUE" +
                    ")");
            TestHelper.streamTable(connection, "dbz4388");

            connection.execute("INSERT INTO debezium.dbz4388 (first_name,last_name,email) values ('John','Doe','john@noanswer.org')");

            // the snapshot process would throw a NPE due to a lowercase PDB or DBNAME setup
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4388")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4388")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ4388").get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("FIRST_NAME")).isEqualTo("John");
            assertThat(after.schema().field("ID").schema().defaultValue()).isNull();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.dbz4388 (first_name,last_name,email) values ('Jane','Doe','jane@noanswer.org')");

            records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4388")).hasSize(1);

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ4388").get(0);
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("FIRST_NAME")).isEqualTo("Jane");
            assertThat(after.schema().field("ID").schema().defaultValue()).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4388");
        }
    }

    private long getOracleIntervalYearMonth(int years, int month) {
        return MicroDuration.durationMicros(years, month, 0, 0, 0, 0, MicroDuration.DAYS_PER_MONTH_AVG);
    }

    private String getOracleIntervalYearMonthString(int years, int month) {
        return Interval.toIsoString(years, month, 0, 0, 0, BigDecimal.ZERO);
    }

    private long getOracleIntervalDaySecond(int days, int hours, int minutes, int seconds, int micros) {
        return MicroDuration.durationMicros(0, 0, days, hours, minutes, seconds, micros, MicroDuration.DAYS_PER_MONTH_AVG);
    }

    private String getOracleIntervalDaySecondString(int days, int hours, int minutes, int seconds, int micros) {
        double secondsDouble = (double) seconds + (double) micros / 1_000_000D;
        return Interval.toIsoString(0, 0, days, hours, minutes, BigDecimal.valueOf(secondsDouble));
    }

    /**
     * Handles executing the full common set of default value tests for the supplied column definitions.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void shouldHandleDefaultValuesCommon(List<ColumnDefinition> columnDefinitions) throws Exception {
        testDefaultValuesCreateTableAndSnapshot(columnDefinitions);
        testDefaultValuesAlterTableModifyExisting(columnDefinitions);
        testDefaultValuesAlterTableAdd(columnDefinitions);
        TestDefaultValuesByRestartAndLoadingHistoryTopic();
    }

    /**
     * Restarts the connector and verifies when the database schema history topic is loaded that we can parse
     * all the loaded history statements without failures.
     */
    private void TestDefaultValuesByRestartAndLoadingHistoryTopic() throws Exception {
        stopConnector();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    /**
     * Creates the table and pre-inserts a record captured during the snapshot phase.  The snapshot
     * record will be validated against the supplied column definitions.
     *
     * The goal of this method is to test that when a table is snapshot which uses default values
     * that both the in-memory schema representation and the snapshot pipeline change event have
     * the right default value resolution.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesCreateTableAndSnapshot(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder createSql = new StringBuilder();
        createSql.append("CREATE TABLE default_value_test (id numeric(9,0) not null");
        for (ColumnDefinition column : columnDefinitions) {
            createSql.append(", ")
                    .append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("default ").append(column.addDefaultValue);
            createSql.append(", ")
                    .append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("default null");
            if (column.temporalType) {
                createSql.append(", ")
                        .append(column.name).append("_sysdate")
                        .append(" ").append(column.definition)
                        .append(" ").append("default sysdate");
                createSql.append(", ")
                        .append(column.name).append("_sysdate_nonnull")
                        .append(" ").append(column.definition)
                        .append(" ").append("default sysdate not null");
            }
        }
        createSql.append(", primary key(id))");

        // Create table and add logging support
        connection.execute(createSql.toString());
        TestHelper.streamTable(connection, "default_value_test");

        // Insert snapshot record
        connection.execute("INSERT INTO default_value_test (id) values (1)");

        // store config so it can be used by other methods
        config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DEFAULT_VALUE_TEST")
                .apply(configUpdater)
                .build();

        // Start connector
        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        // Wait and capture snapshot records
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DEFAULT_VALUE_TEST");
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }

            if (column.temporalType) {
                assertSchemaFieldWithDefaultSysdate(record, column.name.toUpperCase() + "_SYSDATE", null);
                if (column.expectedAddDefaultValue instanceof String) {
                    final String assertionValue = column.isZonedTimestamp() ? "1970-01-01T00:00:00Z" : "0";
                    assertSchemaFieldDefaultAndNonNullValue(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", assertionValue);
                }
                else {
                    assertSchemaFieldWithDefaultSysdate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                }
            }
        }
    }

    /**
     * Alters the underlying table changing the default value to its second form.  This method then inserts
     * a new record that is then validated against the supplied column definitions.
     *
     * The goal of this method is to test that when DDL modifies an existing column in an existing table
     * that the right default value resolution occurs and that the in-memory schema representation is
     * correct as well as the change event capture pipeline.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesAlterTableModifyExisting(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder alterSql = new StringBuilder();
        alterSql.append("ALTER TABLE default_value_test modify (");
        Iterator<ColumnDefinition> iterator = columnDefinitions.iterator();
        while (iterator.hasNext()) {
            final ColumnDefinition column = iterator.next();
            alterSql.append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("default ").append(column.modifyDefaultValue);
            alterSql.append(", ")
                    .append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("default null");
            if (column.temporalType) {
                alterSql.append(", ")
                        .append(column.name).append("_sysdate")
                        .append(" ").append(column.definition)
                        .append(" ").append("default sysdate");
                // cannot add alter for column to not null since it is already not null
                // see creation of table method for where we define this field as not null
            }
            if (iterator.hasNext()) {
                alterSql.append(", ");
            }
        }
        alterSql.append(")");

        // Wait until we're in streaming phase if we're not already
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute(alterSql.toString());
        connection.execute("INSERT INTO default_value_test (id) values (2)");

        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DEFAULT_VALUE_TEST");
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase() + "_NULL", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }

            if (column.temporalType) {
                assertSchemaFieldWithDefaultSysdate(record, column.name.toUpperCase() + "_SYSDATE", null);
                if (column.expectedAddDefaultValue instanceof String) {
                    final String assertionValue = column.isZonedTimestamp() ? "1970-01-01T00:00:00Z" : "0";
                    assertSchemaFieldDefaultAndNonNullValue(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", assertionValue);
                }
                else {
                    assertSchemaFieldWithDefaultSysdate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                }
            }
        }
    }

    /**
     * Alters the underlying table changing adding a new column prefixed with {@code A} to each of the column
     * definition with the initial default value definition.
     *
     * The goal of this method is to test that when DDL adds a new column to an existing table that the right
     * default value resolution occurs and that the in-memory schema representation is correct as well as the
     * change event capture pipeline.
     *
     * @param columnDefinitions list of column definitions, should not be {@code null}
     * @throws Exception if an exception occurred
     */
    private void testDefaultValuesAlterTableAdd(List<ColumnDefinition> columnDefinitions) throws Exception {
        // Build SQL
        final StringBuilder alterSql = new StringBuilder();
        alterSql.append("ALTER TABLE default_value_test add (");
        Iterator<ColumnDefinition> iterator = columnDefinitions.iterator();
        while (iterator.hasNext()) {
            final ColumnDefinition column = iterator.next();
            alterSql.append("a").append(column.name)
                    .append(" ").append(column.definition)
                    .append(" ").append("default ").append(column.addDefaultValue);
            alterSql.append(", ")
                    .append("a").append(column.name).append("_null")
                    .append(" ").append(column.definition)
                    .append(" ").append("default null");
            if (column.temporalType) {
                alterSql.append(", ")
                        .append("a").append(column.name).append("_sysdate")
                        .append(" ").append(column.definition)
                        .append(" ").append("default sysdate");
                alterSql.append(", ")
                        .append("a").append(column.name).append("_sysdate_nonnull")
                        .append(" ").append(column.definition)
                        .append(" ").append("default sysdate not null");
            }
            if (iterator.hasNext()) {
                alterSql.append(", ");
            }
        }
        alterSql.append(")");

        // Wait until we're in streaming phase if we're not already
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute(alterSql.toString());
        connection.execute("INSERT INTO default_value_test (id) values (3)");

        SourceRecords records = consumeRecordsByTopic(1);

        // Verify we got only 1 record for our test
        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DEFAULT_VALUE_TEST");
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);
        for (ColumnDefinition column : columnDefinitions) {
            switch (column.assertionType) {
                case FIELD_DEFAULT_EQUAL:
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, column.name.toUpperCase() + "_NULL", null);
                    assertSchemaFieldWithSameDefaultAndValue(record, "A" + column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldWithSameDefaultAndValue(record, "A" + column.name.toUpperCase() + "_NULL", null);
                    break;
                case FIELD_NO_DEFAULT:
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase(), column.expectedModifyDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, column.name.toUpperCase() + "_NULL", null);
                    assertSchemaFieldNoDefaultWithValue(record, "A" + column.name.toUpperCase(), column.expectedAddDefaultValue);
                    assertSchemaFieldNoDefaultWithValue(record, "A" + column.name.toUpperCase() + "_NULL", null);
                    break;
                default:
                    throw new RuntimeException("Unexpected assertion type: " + column.assertionType);
            }

            if (column.temporalType) {
                assertSchemaFieldWithDefaultSysdate(record, column.name.toUpperCase() + "_SYSDATE", null);
                assertSchemaFieldWithDefaultSysdate(record, "A" + column.name.toUpperCase() + "_SYSDATE", null);
                if (column.expectedAddDefaultValue instanceof String) {
                    final String assertionValue = column.isZonedTimestamp() ? "1970-01-01T00:00:00Z" : "0";
                    assertSchemaFieldDefaultAndNonNullValue(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", assertionValue);
                    assertSchemaFieldDefaultAndNonNullValue(record, "A" + column.name.toUpperCase() + "_SYSDATE_NONNULL", assertionValue);
                }
                else {
                    assertSchemaFieldWithDefaultSysdate(record, column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                    assertSchemaFieldWithDefaultSysdate(record, "A" + column.name.toUpperCase() + "_SYSDATE_NONNULL", 0L);
                }
            }
        }
    }

    /**
     * Asserts that the schema field's default value and after emitted event value are the same.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param expectedValue the expected value in the field's default and "after" struct
     */
    private static void assertSchemaFieldWithSameDefaultAndValue(SourceRecord record, String fieldName, Object expectedValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, expectedValue, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isEqualTo(expectedValue);
        });
    }

    /**
     * Asserts that the schema field's default value is not set and that the emitted event value matches.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param fieldValue the expected value in the field's "after" struct
     */
    // asserts that the field schema has no default value and an emitted value
    private static void assertSchemaFieldNoDefaultWithValue(SourceRecord record, String fieldName, Object fieldValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, null, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isEqualTo(fieldValue);
        });
    }

    /**
     * Asserts that the schema field's default value is the supplied value and that the emitted events field
     * value is at least a non-null value; expectation is that the emitted event value is dynamic, likely
     * based on some database function call, like {@code TO_DATE} or {@code TO_TIMESTAMP}.
     *
     * @param record the change event record, never {@code null}
     * @param fieldName the field name, never {@code null}
     * @param defaultValue the expected schema field's default value
     */
    // asserts that the field schema has a given default value and a non-null emitted event value
    private static void assertSchemaFieldDefaultAndNonNullValue(SourceRecord record, String fieldName, Object defaultValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, defaultValue, r -> {
            assertThat(r).as("Unexpected field value: " + fieldName).isNotNull();
        });
    }

    private static void assertSchemaFieldWithDefaultSysdate(SourceRecord record, String fieldName, Object expectedValue) {
        assertSchemaFieldValueWithDefault(record, fieldName, expectedValue, r -> {
            if (expectedValue == null) {
                assertThat(r).isNull();
            }
            else {
                assertThat((long) r).as("Unexpected field value: " + fieldName).isGreaterThanOrEqualTo(1L);
            }
        });
    }

    private static void assertSchemaFieldValueWithDefault(SourceRecord record, String fieldName, Object expectedDefault, Consumer<Object> valueCheck) {
        final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        final Field field = after.schema().field(fieldName);
        assertThat(field).as("Expected non-null field for " + fieldName).isNotNull();
        final Object defaultValue = field.schema().defaultValue();
        if (expectedDefault == null) {
            assertThat(defaultValue).isNull();
            return;
        }
        else {
            assertThat(defaultValue).as("Expected non-null default value for field " + fieldName).isNotNull();
        }
        assertThat(defaultValue.getClass()).isEqualTo(expectedDefault.getClass());
        assertThat(defaultValue).as("Unexpected default value: " + fieldName + " with field value: " + after.get(fieldName)).isEqualTo(expectedDefault);
        valueCheck.accept(after.get(fieldName));
    }

    /**
     * Defines the different assertion types for a given column definition.
     */
    enum AssertionType {
        // field and default values are identical
        FIELD_DEFAULT_EQUAL,
        // schema has no default value specified
        FIELD_NO_DEFAULT,
    }

    /**
     * Defines a column definition and its attributes that are used by tests.
     */
    private static class ColumnDefinition {
        public final String name;
        public final String definition;
        public final String addDefaultValue;
        public final String modifyDefaultValue;
        public final Object expectedAddDefaultValue;
        public final Object expectedModifyDefaultValue;
        public final AssertionType assertionType;
        public final boolean temporalType;

        ColumnDefinition(String name, String definition, String addDefaultValue, String modifyDefaultValue,
                         Object expectedAddDefaultValue, Object expectedModifyDefaultValue, AssertionType assertionType) {
            this.name = name;
            this.definition = definition;
            this.addDefaultValue = addDefaultValue;
            this.modifyDefaultValue = modifyDefaultValue;
            this.expectedAddDefaultValue = expectedAddDefaultValue;
            this.expectedModifyDefaultValue = expectedModifyDefaultValue;
            this.assertionType = assertionType;
            this.temporalType = definition.equalsIgnoreCase("date") || definition.toUpperCase().startsWith("TIMESTAMP");
        }

        public boolean isZonedTimestamp() {
            return definition.equalsIgnoreCase("timestamp with time zone")
                    || definition.equalsIgnoreCase("timestamp with local time zone");
        }
    }
}
