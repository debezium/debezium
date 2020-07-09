/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractOracleDatatypesTest extends AbstractConnectorTest {

    /**
     * Key for schema parameter used to store DECIMAL/NUMERIC columns' precision.
     */
    static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    private static final Schema NUMBER_SCHEMA = Decimal.builder(0).optional().parameter(PRECISION_PARAMETER_KEY, "38").build();

    private static final String DDL_STRING = "create table debezium.type_string (" +
            "  id numeric(9,0) not null, " +
            "  val_varchar varchar(1000), " +
            "  val_varchar2 varchar2(1000), " +
            "  val_nvarchar2 nvarchar2(1000), " +
            "  val_char char(3), " +
            "  val_nchar nchar(3), " +
            "  primary key (id)" +
            ")";

    private static final String DDL_FP = "create table debezium.type_fp (" +
            "  id numeric(9,0) not null, " +
            "  val_bf binary_float, " +
            "  val_bd binary_double, " +
            "  val_f float, " +
            "  val_f_10 float (10), " +
            "  val_num number(10,6), " +
            "  val_dp double precision, " +
            "  val_r real, " +
            "  val_decimal decimal(10, 6), " +
            "  val_numeric numeric(10, 6), " +
            "  val_num_vs number, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_INT = "create table debezium.type_int (" +
            "  id numeric(9,0) not null, " +
            "  val_int int, " +
            "  val_integer integer, " +
            "  val_smallint smallint, " +
            "  val_number_38_no_scale number(38), " +
            "  val_number_38_scale_0 number(38, 0), " +
            "  val_number_2 number(2), " +
            "  val_number_4 number(4), " +
            "  val_number_9 number(9), " +
            "  val_number_18 number(18), " +
            "  val_number_2_negative_scale number(1, -1), " +
            "  val_number_4_negative_scale number(2, -2), " +
            "  val_number_9_negative_scale number(8, -1), " +
            "  val_number_18_negative_scale number(16, -2), " +
            "  val_decimal decimal(10), " +
            "  val_numeric numeric(10), " +
            "  primary key (id)" +
            ")";

    private static final String DDL_TIME = "create table debezium.type_time (" +
            "  id numeric(9,0) not null, " +
            "  val_date date, " +
            "  val_ts timestamp, " +
            "  val_ts_precision2 timestamp(2), " +
            "  val_ts_precision4 timestamp(4), " +
            "  val_tstz timestamp with time zone, " +
            "  val_tsltz timestamp with local time zone, " +
            "  val_int_ytm interval year to month, " +
            "  val_int_dts interval day(3) to second(2), " +
            "  primary key (id)" +
            ")";

    private static final List<SchemaAndValueField> EXPECTED_STRING = Arrays.asList(
            new SchemaAndValueField("VAL_VARCHAR", Schema.OPTIONAL_STRING_SCHEMA, "v\u010d2"),
            new SchemaAndValueField("VAL_VARCHAR2", Schema.OPTIONAL_STRING_SCHEMA, "v\u010d2"),
            new SchemaAndValueField("VAL_NVARCHAR2", Schema.OPTIONAL_STRING_SCHEMA, "nv\u010d2"),
            new SchemaAndValueField("VAL_CHAR", Schema.OPTIONAL_STRING_SCHEMA, "c  "),
            new SchemaAndValueField("VAL_NCHAR", Schema.OPTIONAL_STRING_SCHEMA, "n\u010d "));

    private static final List<SchemaAndValueField> EXPECTED_FP = Arrays.asList(
            new SchemaAndValueField("VAL_BF", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("VAL_BD", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22),
            new SchemaAndValueField("VAL_F", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("3.33"))),
            new SchemaAndValueField("VAL_F_10", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("8.888"))),
            new SchemaAndValueField("VAL_NUM", Decimal.builder(6).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("4.444400")),
            new SchemaAndValueField("VAL_DP", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("5.555"))),
            new SchemaAndValueField("VAL_R", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("6.66"))),
            new SchemaAndValueField("VAL_DECIMAL", Decimal.builder(6).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("1234.567891")),
            new SchemaAndValueField("VAL_NUMERIC", Decimal.builder(6).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("1234.567891")),
            new SchemaAndValueField("VAL_NUM_VS", VariableScaleDecimal.builder().optional().build(),
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("77.323"))));

    private static final List<SchemaAndValueField> EXPECTED_FP_AS_STRING = Arrays.asList(
            new SchemaAndValueField("VAL_BF", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("VAL_BD", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22),
            new SchemaAndValueField("VAL_F", Schema.OPTIONAL_STRING_SCHEMA, "3.33"),
            new SchemaAndValueField("VAL_F_10", Schema.OPTIONAL_STRING_SCHEMA, "8.888"),
            new SchemaAndValueField("VAL_NUM", Schema.OPTIONAL_STRING_SCHEMA, "4.444400"),
            new SchemaAndValueField("VAL_DP", Schema.OPTIONAL_STRING_SCHEMA, "5.555"),
            new SchemaAndValueField("VAL_R", Schema.OPTIONAL_STRING_SCHEMA, "6.66"),
            new SchemaAndValueField("VAL_DECIMAL", Schema.OPTIONAL_STRING_SCHEMA, "1234.567891"),
            new SchemaAndValueField("VAL_NUMERIC", Schema.OPTIONAL_STRING_SCHEMA, "1234.567891"),
            new SchemaAndValueField("VAL_NUM_VS", Schema.OPTIONAL_STRING_SCHEMA, "77.323"));

    private static final List<SchemaAndValueField> EXPECTED_FP_AS_DOUBLE = Arrays.asList(
            new SchemaAndValueField("VAL_BF", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("VAL_BD", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22),
            new SchemaAndValueField("VAL_F", Schema.OPTIONAL_FLOAT64_SCHEMA, 3.33),
            new SchemaAndValueField("VAL_F_10", Schema.OPTIONAL_FLOAT64_SCHEMA, 8.888),
            new SchemaAndValueField("VAL_NUM", Schema.OPTIONAL_FLOAT64_SCHEMA, 4.4444),
            new SchemaAndValueField("VAL_DP", Schema.OPTIONAL_FLOAT64_SCHEMA, 5.555),
            new SchemaAndValueField("VAL_R", Schema.OPTIONAL_FLOAT64_SCHEMA, 6.66),
            new SchemaAndValueField("VAL_DECIMAL", Schema.OPTIONAL_FLOAT64_SCHEMA, 1234.567891),
            new SchemaAndValueField("VAL_NUMERIC", Schema.OPTIONAL_FLOAT64_SCHEMA, 1234.567891),
            new SchemaAndValueField("VAL_NUM_VS", Schema.OPTIONAL_FLOAT64_SCHEMA, 77.323));

    private static final List<SchemaAndValueField> EXPECTED_INT = Arrays.asList(
            new SchemaAndValueField("VAL_INT", NUMBER_SCHEMA, new BigDecimal("1")),
            new SchemaAndValueField("VAL_INTEGER", NUMBER_SCHEMA, new BigDecimal("22")),
            new SchemaAndValueField("VAL_SMALLINT", NUMBER_SCHEMA, new BigDecimal("333")),
            new SchemaAndValueField("VAL_NUMBER_38_NO_SCALE", NUMBER_SCHEMA, new BigDecimal("4444")),
            new SchemaAndValueField("VAL_NUMBER_38_SCALE_0", NUMBER_SCHEMA, new BigDecimal("5555")),
            new SchemaAndValueField("VAL_NUMBER_2", Schema.OPTIONAL_INT8_SCHEMA, (byte) 99),
            new SchemaAndValueField("VAL_NUMBER_4", Schema.OPTIONAL_INT16_SCHEMA, (short) 9999),
            new SchemaAndValueField("VAL_NUMBER_9", Schema.OPTIONAL_INT32_SCHEMA, 9999_99999),
            new SchemaAndValueField("VAL_NUMBER_18", Schema.OPTIONAL_INT64_SCHEMA, 999_99999_99999_99999L),
            new SchemaAndValueField("VAL_NUMBER_2_NEGATIVE_SCALE", Schema.OPTIONAL_INT8_SCHEMA, (byte) 90),
            new SchemaAndValueField("VAL_NUMBER_4_NEGATIVE_SCALE", Schema.OPTIONAL_INT16_SCHEMA, (short) 9900),
            new SchemaAndValueField("VAL_NUMBER_9_NEGATIVE_SCALE", Schema.OPTIONAL_INT32_SCHEMA, 9999_99990),
            new SchemaAndValueField("VAL_NUMBER_18_NEGATIVE_SCALE", Schema.OPTIONAL_INT64_SCHEMA, 999_99999_99999_99900L),
            new SchemaAndValueField("VAL_DECIMAL", Schema.OPTIONAL_INT64_SCHEMA, 99999_99999L),
            new SchemaAndValueField("VAL_NUMERIC", Schema.OPTIONAL_INT64_SCHEMA, 99999_99999L));

    private static final List<SchemaAndValueField> EXPECTED_TIME = Arrays.asList(
            new SchemaAndValueField("VAL_DATE", Timestamp.builder().optional().build(), 1522108800_000l),
            new SchemaAndValueField("VAL_TS", MicroTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890),
            new SchemaAndValueField("VAL_TS_PRECISION2", Timestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130),
            new SchemaAndValueField("VAL_TS_PRECISION4", MicroTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500),
            new SchemaAndValueField("VAL_TSTZ", ZonedTimestamp.builder().optional().build(), "2018-03-27T01:34:56.00789-11:00"),
            new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000L),
            new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000L)
    // new SchemaAndValueField("VAL_TSLTZ", ZonedTimestamp.builder().optional().build(), "2018-03-27T01:34:56.00789-11:00")
    );

    private static final String[] ALL_TABLES = {
            "debezium.type_string",
            "debezium.type_fp",
            "debezium.type_int",
            "debezium.type_time"
    };

    private static final String[] ALL_DDLS = {
            DDL_STRING,
            DDL_FP,
            DDL_INT,
            DDL_TIME
    };

    private static OracleConnection connection;

    @BeforeClass
    public static void dropTables() throws SQLException {
        connection = TestHelper.testConnection();
        for (String table : ALL_TABLES) {
            TestHelper.dropTable(connection, table);
        }
    }

    protected static void createTables() throws SQLException {
        connection.execute(ALL_DDLS);
        for (String table : ALL_TABLES) {
            streamTable(table);
        }
    }

    protected List<String> getAllTables() {
        return Arrays.asList(ALL_TABLES);
    }

    protected abstract boolean insertRecordsDuringTest();

    protected abstract Builder connectorConfig();

    private static void streamTable(String table) throws SQLException {
        connection.execute("GRANT SELECT ON " + table + " to " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE " + table + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void stringTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertStringTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_STRING");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, "ID", 1);
        }
        else {
            VerifyRecord.isValidRead(record, "ID", 1);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_STRING);
    }

    @Test
    public void fpTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertFpTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_FP");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, "ID", 1);
        }
        else {
            VerifyRecord.isValidRead(record, "ID", 1);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP);
    }

    @Test
    @FixFor("DBZ-1552")
    public void fpTypesAsString() throws Exception {
        stopConnector();
        initializeConnectorTestFramework();
        final Configuration config = connectorConfig()
                .with(OracleConnectorConfig.DECIMAL_HANDLING_MODE, DecimalMode.STRING)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertFpTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_FP");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, "ID", 1);
        }
        else {
            VerifyRecord.isValidRead(record, "ID", 1);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP_AS_STRING);
    }

    @Test
    @FixFor("DBZ-1552")
    public void fpTypesAsDouble() throws Exception {
        stopConnector();
        initializeConnectorTestFramework();
        final Configuration config = connectorConfig()
                .with(OracleConnectorConfig.DECIMAL_HANDLING_MODE, DecimalMode.DOUBLE)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertFpTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_FP");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, "ID", 1);
        }
        else {
            VerifyRecord.isValidRead(record, "ID", 1);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP_AS_DOUBLE);
    }

    @Test
    public void intTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertIntTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_INT");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, "ID", 1);
        }
        else {
            VerifyRecord.isValidRead(record, "ID", 1);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_INT);
    }

    @Test
    public void timeTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertTimeTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_TIME");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        SourceRecord record = testTableRecords.get(0);

        VerifyRecord.isValid(record);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(record, "ID", 1);
        }
        else {
            VerifyRecord.isValidRead(record, "ID", 1);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_TIME);
    }

    protected static void insertStringTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_string VALUES (1, 'v\u010d2', 'v\u010d2', 'nv\u010d2', 'c', 'n\u010d')");
        connection.execute("COMMIT");
    }

    protected static void insertFpTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_fp VALUES (1, 1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323)");
        connection.execute("COMMIT");
    }

    protected static void insertIntTypes() throws SQLException {
        connection.execute(
                "INSERT INTO debezium.type_int VALUES (1, 1, 22, 333, 4444, 5555, 99, 9999, 999999999, 999999999999999999, 94, 9949, 999999994, 999999999999999949, 9999999999, 9999999999)");
        connection.execute("COMMIT");
    }

    protected static void insertTimeTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_time VALUES ("
                + "1"
                + ", TO_DATE('27-MAR-2018', 'dd-MON-yyyy')"
                + ", TO_TIMESTAMP('27-MAR-2018 12:34:56.00789', 'dd-MON-yyyy HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP('27-MAR-2018 12:34:56.12545', 'dd-MON-yyyy HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP('27-MAR-2018 12:34:56.12545', 'dd-MON-yyyy HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP_TZ('27-MAR-2018 01:34:56.00789 -11:00', 'dd-MON-yyyy HH24:MI:SS.FF5 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('27-MAR-2018 01:34:56.00789', 'dd-MON-yyyy HH24:MI:SS.FF5')"
                + ", INTERVAL '-3-6' YEAR TO MONTH"
                + ", INTERVAL '-1 2:3:4.56' DAY TO SECOND"
                + ")");
        connection.execute("COMMIT");
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
