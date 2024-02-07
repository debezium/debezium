/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.data.VariableScaleDecimal.fromLogical;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Only applies to LogMiner")
public class HybridMiningStrategyIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();
    }

    @After
    public void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.dropAllTables();
            connection.close();
        }
    }

    // todo:
    // add test for having old non-attribute populated schema history and status=2 triggered
    // add integer for 36_negative_scale using (36,-2)
    // do we need tests for connect precision or other handling modes, doubtful but should check?
    // add clob, blob, and xml support

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangesCharacterDataTypes() throws Exception {
        streamOfflineSchemaChanges("varchar(50)", "ABC", "XYZ", "ABC", "XYZ");
        streamOfflineSchemaChanges("varchar2(50)", "ABC", "XYZ", "ABC", "XYZ");
        streamOfflineSchemaChanges("nvarchar2(50)", "AêñüC", "XYZ", "AêñüC", "XYZ");
        streamOfflineSchemaChanges("char(3)", "NO", "YES", "NO ", "YES");
        streamOfflineSchemaChanges("nchar(3)", "NO", "YES", "NO ", "YES");
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithDataChangeCharacterDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("varchar(50)", "ABC", "XYZ", "ABC", "XYZ");
        streamSchemaChangeMixedWithDataChange("varchar2(50)", "ABC", "XYZ", "ABC", "XYZ");
        streamSchemaChangeMixedWithDataChange("nvarchar2(50)", "AêñüC", "XYZ", "AêñüC", "XYZ");
        streamSchemaChangeMixedWithDataChange("char(3)", "NO", "YES", "NO ", "YES");
        streamSchemaChangeMixedWithDataChange("nchar(3)", "NO", "YES", "NO ", "YES");
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangesFloatingPointDataTypes() throws Exception {
        streamOfflineSchemaChanges("binary_float", 3.14f, 4.14f, 3.14f, 4.14f);
        streamOfflineSchemaChanges("binary_double", 3.14, 4.14, 3.14, 4.14);
        streamOfflineSchemaChanges("float", 3.33, 4.33, varScaleDecimal("3.33"), varScaleDecimal("4.33"));
        streamOfflineSchemaChanges("float(10)", 8.888, 9.999, varScaleDecimal("8.888"), varScaleDecimal("9.999"));
        streamOfflineSchemaChanges("number(10,6)", 4.4444, 5.5555, new BigDecimal("4.444400"), new BigDecimal("5.555500"));
        streamOfflineSchemaChanges("double precision", 5.555, 6.666, varScaleDecimal("5.555"), varScaleDecimal("6.666"));
        streamOfflineSchemaChanges("real", 6.66, 7.77, varScaleDecimal("6.66"), varScaleDecimal("7.77"));
        streamOfflineSchemaChanges("decimal(10,6)", 1234.567891, 2345.678912, new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamOfflineSchemaChanges("numeric(10,6)", 1234.567891, 2345.678912, new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamOfflineSchemaChanges("number", 77.323, 88.434, varScaleDecimal("77.323"), varScaleDecimal("88.434"));
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithDataChangeFloatingPointDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("binary_float", 3.14f, 4.14f, 3.14f, 4.14f);
        streamSchemaChangeMixedWithDataChange("binary_double", 3.14, 4.14, 3.14, 4.14);
        streamSchemaChangeMixedWithDataChange("float", 3.33, 4.33, varScaleDecimal("3.33"), varScaleDecimal("4.33"));
        streamSchemaChangeMixedWithDataChange("float(10)", 8.888, 9.999, varScaleDecimal("8.888"), varScaleDecimal("9.999"));
        streamSchemaChangeMixedWithDataChange("number(10,6)", 4.4444, 5.5555, new BigDecimal("4.444400"), new BigDecimal("5.555500"));
        streamSchemaChangeMixedWithDataChange("double precision", 5.555, 6.666, varScaleDecimal("5.555"), varScaleDecimal("6.666"));
        streamSchemaChangeMixedWithDataChange("real", 6.66, 7.77, varScaleDecimal("6.66"), varScaleDecimal("7.77"));
        streamSchemaChangeMixedWithDataChange("decimal(10,6)", 1234.567891, 2345.678912, new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamSchemaChangeMixedWithDataChange("numeric(10,6)", 1234.567891, 2345.678912, new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamSchemaChangeMixedWithDataChange("number", 77.323, 88.434, varScaleDecimal("77.323"), varScaleDecimal("88.434"));
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangesIntegerDataTypes() throws Exception {
        streamOfflineSchemaChanges("int", 1, 2, new BigDecimal("1"), new BigDecimal("2"));
        streamOfflineSchemaChanges("integer", 1, 2, new BigDecimal("1"), new BigDecimal("2"));
        streamOfflineSchemaChanges("smallint", 33, 44, new BigDecimal("33"), new BigDecimal("44"));
        streamOfflineSchemaChanges("number(38)", 4444, 5555, new BigDecimal("4444"), new BigDecimal("5555"));
        streamOfflineSchemaChanges("number(38,0)", 4444, 5555, new BigDecimal("4444"), new BigDecimal("5555"));
        streamOfflineSchemaChanges("number(2)", 88, 99, (byte) 88, (byte) 99);
        streamOfflineSchemaChanges("number(4)", 8888, 9999, (short) 8888, (short) 9999);
        streamOfflineSchemaChanges("number(9)", 888888888, 999999999, 888888888, 999999999);
        streamOfflineSchemaChanges("number(18)", 888888888888888888L, 999999999999999999L, 888888888888888888L, 999999999999999999L);
        streamOfflineSchemaChanges("number(1,-1)", 93, 94, (byte) 90, (byte) 90);
        streamOfflineSchemaChanges("number(2,-2)", 9349, 9449, (short) 9300, (short) 9400);
        streamOfflineSchemaChanges("number(8,-1)", 989999994, 999999994, 989999990, 999999990);
        streamOfflineSchemaChanges("number(16,-2)", 989999999999999949L, 999999999999999949L, 989999999999999900L, 999999999999999900L);
        streamOfflineSchemaChanges("decimal(10)", 9899999999L, 9999999999L, 9899999999L, 9999999999L);
        streamOfflineSchemaChanges("numeric(10)", 9899999999L, 9999999999L, 9899999999L, 9999999999L);
        streamOfflineSchemaChanges("number(1)", 1, 2, (byte) 1, (byte) 2);
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithDataChangeIntegerDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("int", 1, 2, new BigDecimal("1"), new BigDecimal("2"));
        streamSchemaChangeMixedWithDataChange("integer", 1, 2, new BigDecimal("1"), new BigDecimal("2"));
        streamSchemaChangeMixedWithDataChange("smallint", 33, 44, new BigDecimal("33"), new BigDecimal("44"));
        streamSchemaChangeMixedWithDataChange("number(38)", 4444, 5555, new BigDecimal("4444"), new BigDecimal("5555"));
        streamSchemaChangeMixedWithDataChange("number(38,0)", 4444, 5555, new BigDecimal("4444"), new BigDecimal("5555"));
        streamSchemaChangeMixedWithDataChange("number(2)", 88, 99, (byte) 88, (byte) 99);
        streamSchemaChangeMixedWithDataChange("number(4)", 8888, 9999, (short) 8888, (short) 9999);
        streamSchemaChangeMixedWithDataChange("number(9)", 888888888, 999999999, 888888888, 999999999);
        streamSchemaChangeMixedWithDataChange("number(18)", 888888888888888888L, 999999999999999999L, 888888888888888888L, 999999999999999999L);
        streamSchemaChangeMixedWithDataChange("number(1,-1)", 93, 94, (byte) 90, (byte) 90);
        streamSchemaChangeMixedWithDataChange("number(2,-2)", 9349, 9449, (short) 9300, (short) 9400);
        streamSchemaChangeMixedWithDataChange("number(8,-1)", 989999994, 999999994, 989999990, 999999990);
        streamSchemaChangeMixedWithDataChange("number(16,-2)", 989999999999999949L, 999999999999999949L, 989999999999999900L, 999999999999999900L);
        streamSchemaChangeMixedWithDataChange("decimal(10)", 9899999999L, 9999999999L, 9899999999L, 9999999999L);
        streamSchemaChangeMixedWithDataChange("numeric(10)", 9899999999L, 9999999999L, 9899999999L, 9999999999L);
        streamSchemaChangeMixedWithDataChange("number(1)", 1, 2, (byte) 1, (byte) 2);
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangesTemporalDataTypes() throws Exception {
        streamOfflineSchemaChanges("date",
                "TO_DATE('2018-03-27','yyyy-mm-dd')",
                "TO_DATE('2018-10-15','yyyy-mm-dd')",
                1_522_108_800_000L,
                1_539_561_600_000L);
        streamOfflineSchemaChanges("timestamp",
                toTimestamp(2018, 3, 27, 12, 34, 56, 789, 5),
                toTimestamp(2018, 10, 15, 12, 34, 56, 789, 5),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890);
        streamOfflineSchemaChanges("timestamp(2)",
                toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5),
                toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130);
        streamOfflineSchemaChanges("timestamp(4)",
                toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5),
                toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500);
        streamOfflineSchemaChanges("timestamp(9)",
                toTimestamp(2018, 3, 27, 12, 34, 56, 123456789, 9),
                toTimestamp(2018, 10, 15, 12, 34, 56, 123456789, 9),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789);
        streamOfflineSchemaChanges("timestamp with time zone",
                toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-11:00"),
                toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-11:00"),
                "2018-03-27T01:34:56.007890-11:00",
                "2018-10-15T01:34:56.007890-11:00");
        streamOfflineSchemaChanges("timestamp with local time zone",
                toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-06:00"),
                toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-06:00"),
                "2018-03-27T07:34:56.007890Z",
                "2018-10-15T07:34:56.007890Z");
        streamOfflineSchemaChanges("interval year to month",
                "INTERVAL '-3-6' YEAR TO MONTH",
                "INTERVAL '-2-5' YEAR TO MONTH",
                -110_451_600_000_000L,
                -76_264_200_000_000L);
        streamOfflineSchemaChanges("interval day(3) to second(2)",
                "INTERVAL '-1 2:3:4.56' DAY TO SECOND",
                "INTERVAL '-2 4:5:6.21' DAY TO SECOND",
                -93_784_560_000L,
                -187_506_210_000L);
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithDataChangeTemporalDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("date",
                "TO_DATE('2018-03-27','yyyy-mm-dd')",
                "TO_DATE('2018-10-15','yyyy-mm-dd')",
                1_522_108_800_000L,
                1_539_561_600_000L);
        streamSchemaChangeMixedWithDataChange("timestamp",
                toTimestamp(2018, 3, 27, 12, 34, 56, 789, 5),
                toTimestamp(2018, 10, 15, 12, 34, 56, 789, 5),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890);
        streamSchemaChangeMixedWithDataChange("timestamp(2)",
                toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5),
                toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130);
        streamSchemaChangeMixedWithDataChange("timestamp(4)",
                toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5),
                toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500);
        streamSchemaChangeMixedWithDataChange("timestamp(9)",
                toTimestamp(2018, 3, 27, 12, 34, 56, 123456789, 9),
                toTimestamp(2018, 10, 15, 12, 34, 56, 123456789, 9),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789);
        streamSchemaChangeMixedWithDataChange("timestamp with time zone",
                toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-11:00"),
                toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-11:00"),
                "2018-03-27T01:34:56.007890-11:00",
                "2018-10-15T01:34:56.007890-11:00");
        streamSchemaChangeMixedWithDataChange("timestamp with local time zone",
                toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-06:00"),
                toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-06:00"),
                "2018-03-27T07:34:56.007890Z",
                "2018-10-15T07:34:56.007890Z");
        streamSchemaChangeMixedWithDataChange("interval year to month",
                "INTERVAL '-3-6' YEAR TO MONTH",
                "INTERVAL '-2-5' YEAR TO MONTH",
                -110_451_600_000_000L,
                -76_264_200_000_000L);
        streamSchemaChangeMixedWithDataChange("interval day(3) to second(2)",
                "INTERVAL '-1 2:3:4.56' DAY TO SECOND",
                "INTERVAL '-2 4:5:6.21' DAY TO SECOND",
                -93_784_560_000L,
                -187_506_210_000L);
    }

    @SuppressWarnings("SameParameterValue")
    private static String toTimestamp(int year, int month, int day, int hour, int min, int sec, int nanos, int precision) {
        String nanoSeconds = Strings.justify(Strings.Justify.RIGHT, String.valueOf(nanos), precision, '0');
        String format = "'%04d-%02d-%02d %02d:%02d:%02d.%s', 'yyyy-mm-dd HH24:MI:SS.FF" + precision + "'";
        return String.format("TO_TIMESTAMP(" + format + ")", year, month, day, hour, min, sec, nanoSeconds);
    }

    @SuppressWarnings("SameParameterValue")
    private static String toTimestampTz(int year, int month, int day, int hour, int min, int sec, int nanos, int precision, String tz) {
        String nanoSeconds = Strings.justify(Strings.Justify.RIGHT, String.valueOf(nanos), precision, '0');
        String format = "'%04d-%02d-%02d %02d:%02d:%02d.%s %s', 'yyyy-mm-dd HH24:MI:SS.FF" + precision + " TZH:TZM'";
        return String.format("TO_TIMESTAMP_TZ(" + format + ")", year, month, day, hour, min, sec, nanoSeconds, tz);
    }

    private void streamOfflineSchemaChanges(String columnType, Object insertValue, Object updateValue,
                                            Object expectedInsert, Object expectedUpdate)
            throws Exception {
        streamOfflineSchemaChanges(columnType, insertValue, updateValue, expectedInsert, expectedUpdate, false, false);
        streamOfflineSchemaChanges(columnType, insertValue, updateValue, expectedInsert, expectedUpdate, true, false);
        streamOfflineSchemaChanges(columnType, insertValue, updateValue, expectedInsert, expectedUpdate, true, true);
    }

    @SuppressWarnings("SameParameterValue")
    private void streamSchemaChangeMixedWithDataChange(String columnType, Object insertValue, Object updateValue,
                                                       Object expectedInsert, Object expectedUpdate)
            throws Exception {
        final String columnName = "C1";
        TestHelper.dropTable(connection, "dbz3401");
        try {
            createAndStreamTable(columnName, columnType);

            configureAndStartConnector(false);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // insert streaming record
            insertRowWithoutCommit(columnName, insertValue, 1);
            // add a new column to trigger a schema change
            connection.execute("ALTER TABLE dbz3401 add C2 varchar2(50)");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);
            assertThat(after.schema().field("C2")).isNull(); // field was added after insert

            // update streaming record
            updateRowWithoutCommit(columnName, updateValue, 1);
            // add a new column to trigger a schema change
            connection.execute("ALTER TABLE dbz3401 add C3 varchar2(50)");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedUpdate);
            assertThat(after.get("C2")).isNull();
            assertThat(after.schema().field("C3")).isNull(); // field was added after update

            // delete streaming record
            connection.executeWithoutCommitting("DELETE FROM dbz3401 where id = 1");
            connection.execute("ALTER TABLE dbz3401 add C4 varchar2(50)");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            Struct before = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get(columnName.toUpperCase())).isEqualTo(expectedUpdate);
            assertThat(before.get("C2")).isNull();
            assertThat(before.get("C3")).isNull();

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Perform DML and then DDL (drop table within same scope)
            insertRowWithoutCommit(columnName, insertValue, 2);
            // This test case does not use PURGE so that the table gets pushed into the Oracle RECYCLEBIN
            // LogMiner materializes table name as "ORCLPDB1.DEBEZIUM.BIN$<base64>==$0"
            connection.execute("DROP TABLE dbz3401");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNotNull();
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);

            // Now lets test re-creating the table in-flight
            // This should automatically capture the schema object details
            createAndStreamTable(columnName, columnType);

            // Perform DML and then DDL (drop table within same scope)
            insertRowWithoutCommit(columnName, insertValue, 3);
            // This test case uses PURGE.
            // LogMiner materializes table name as "ORCLPDB1.UNKNOWN.OBJ# <num>"
            connection.execute("DROP TABLE dbz3401 PURGE");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNotNull();
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);

            stopConnector();
        }
        finally {
            // Shutdown the connector explicitly
            stopConnector();

            // drop the table in case of a failure
            TestHelper.dropTable(connection, "dbz3401");

            // cleanup state from multiple invocations
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
            Testing.Files.delete(OFFSET_STORE_PATH);
        }
    }

    private void streamOfflineSchemaChanges(String columnType, Object insertValue, Object updateValue,
                                            Object expectedInsert, Object expectedUpdate,
                                            boolean dropTable, boolean dropTableWithPurge)
            throws Exception {
        final String columnName = "C1";

        TestHelper.dropTable(connection, "dbz3401");
        try {
            // create table & stream it
            createAndStreamTable(columnName, columnType);

            // insert snapshot record
            insertRowWithoutCommit(columnName, insertValue, 1);
            connection.commit();

            Configuration config = configureAndStartConnector(false);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            stopConnector();

            // Do offline actions
            connection.execute("ALTER TABLE dbz3401 ADD " + columnName + "2 " + columnType);
            insertRowOffline(columnName, insertValue, 2);
            connection.execute("ALTER TABLE dbz3401 DROP COLUMN " + columnName + "2");
            updateRowOffline(columnName, updateValue, 2);
            connection.execute("ALTER TABLE dbz3401 ADD " + columnName + "2 " + columnType);
            connection.execute("DELETE FROM dbz3401 WHERE ID = 2");
            connection.execute("ALTER TABLE dbz3401 DROP COLUMN " + columnName + "2");

            if (dropTable) {
                if (dropTableWithPurge) {
                    connection.execute("DROP TABLE dbz3401 PURGE");
                }
                else {
                    TestHelper.dropTable(connection, "dbz3401");
                }
            }

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final int expected = 3;
            records = consumeRecordsByTopic(expected);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));

            // Insert
            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName)).isEqualTo(expectedInsert);
            assertThat(after.get(columnName + "2")).isEqualTo(expectedInsert);

            // Update
            after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName)).isEqualTo(expectedUpdate);
            assertThat(after.schema().field(columnName + "2")).isNull();

            // Delete
            Struct before = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.BEFORE);
            after = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(before.get("ID")).isEqualTo(2);
            assertThat(before.get(columnName)).isEqualTo(expectedUpdate);
            assertThat(before.get(columnName + "2")).isNull();
            assertThat(after).isNull();
        }
        finally {
            // Shutdown the connector explicitly
            stopConnector();

            // drop the table in case of a failure
            TestHelper.dropTable(connection, "dbz3401");

            // cleanup state from multiple invocations
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
            Testing.Files.delete(OFFSET_STORE_PATH);
        }
    }

    private Struct varScaleDecimal(String value) {
        return fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal(value));
    }

    @SuppressWarnings("SameParameterValue")
    private static String topicName(String schema, String table) {
        return TestHelper.SERVER_NAME + "." + schema + "." + table;
    }

    @SuppressWarnings("SameParameterValue")
    private void createAndStreamTable(String columnName, String columnType) throws SQLException {
        // create table & stream it
        connection.execute(String.format("CREATE TABLE dbz3401 (id numeric(9,0) not null primary key, %s %s)",
                columnName, columnType));
        TestHelper.streamTable(connection, "dbz3401");
    }

    @SuppressWarnings("SameParameterValue")
    private Configuration configureAndStartConnector(boolean lobEnabled) {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3401")
                // we explicitly want to target this strategy
                .with(OracleConnectorConfig.LOB_ENABLED, Boolean.toString(lobEnabled))
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "hybrid")
                .with(OracleConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        return config;
    }

    @SuppressWarnings("SameParameterValue")
    private void insertRowWithoutCommit(String columnName, Object insertValue, Integer id) throws SQLException {
        connection.prepareUpdate(
                String.format("INSERT INTO dbz3401 (id,%s) values (%d,?)", columnName, id),
                p -> p.setObject(1, insertValue));
    }

    @SuppressWarnings("SameParameterValue")
    private void updateRowWithoutCommit(String columnName, Object updateValue, Integer id) throws SQLException {
        connection.prepareUpdate(
                String.format("UPDATE dbz3401 set %s=? where id=%d", columnName, id),
                p -> p.setObject(1, updateValue));
    }

    @SuppressWarnings("SameParameterValue")
    private void insertRowOffline(String columnName, Object insertValue, Integer id) throws SQLException {
        connection.prepareUpdate(
                String.format("INSERT INTO dbz3401 (id,%s,%s2) values (%d,?,?)", columnName, columnName, id),
                p -> {
                    p.setObject(1, insertValue);
                    p.setObject(2, insertValue);
                });
        connection.commit();
    }

    @SuppressWarnings("SameParameterValue")
    private void updateRowOffline(String columnName, Object updateValue, Integer id) throws SQLException {
        connection.prepareUpdate(
                String.format("UPDATE dbz3401 SET %s=? where id=%d", columnName, id),
                p -> p.setObject(1, updateValue));
        connection.commit();
    }
}
