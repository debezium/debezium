/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipTestWhenRunWithApicurioRule;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.junit.SkipWhenRunWithApicurio;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractOracleDatatypesTest extends AbstractAsyncEngineConnectorTest {

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
            "  val_num_vs2 number(38,0), " +
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
            "  val_number_36_negative_scale number(36, -2), " +
            "  val_decimal decimal(10), " +
            "  val_numeric numeric(10), " +
            "  val_number_1 number(1), " +
            "  primary key (id)" +
            ")";

    private static final String DDL_TIME = "create table debezium.type_time (" +
            "  id numeric(9,0) not null, " +
            "  val_date date, " +
            "  val_ts timestamp, " +
            "  val_ts_precision2 timestamp(2), " +
            "  val_ts_precision4 timestamp(4), " +
            "  val_ts_precision9 timestamp(9), " +
            "  val_tstz timestamp with time zone, " +
            "  val_tsltz timestamp with local time zone, " +
            "  val_int_ytm interval year to month, " +
            "  val_int_dts interval day(3) to second(2), " +
            "  val_max_date date, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_CLOB = "create table debezium.type_clob (" +
            "  id numeric(9,0) not null, " +
            "  val_clob_inline clob, " +
            "  val_nclob_inline nclob, " +
            "  val_clob_short clob, " +
            "  val_nclob_short nclob, " +
            "  val_clob_long clob, " +
            "  val_nclob_long nclob, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_GEOMETRY = "create table debezium.type_geometry (" +
            "  id numeric(9,0) not null, " +
            "  location sdo_geometry, " +
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
                    VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal("77.323"))),
            new SchemaAndValueField("VAL_NUM_VS2", Decimal.builder(0).parameter(PRECISION_PARAMETER_KEY, "38").optional().build(), new BigDecimal("77")));

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
            new SchemaAndValueField("VAL_NUM_VS", Schema.OPTIONAL_STRING_SCHEMA, "77.323"),
            new SchemaAndValueField("VAL_NUM_VS2", Schema.OPTIONAL_STRING_SCHEMA, "77"));

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
            new SchemaAndValueField("VAL_NUM_VS", Schema.OPTIONAL_FLOAT64_SCHEMA, 77.323),
            new SchemaAndValueField("VAL_NUM_VS2", Schema.OPTIONAL_FLOAT64_SCHEMA, 77.0));

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
            new SchemaAndValueField("VAL_NUMBER_36_NEGATIVE_SCALE", Decimal.builder(-2).optional()
                    .parameter(PRECISION_PARAMETER_KEY, "36").build(),
                    new BigDecimal(
                            new BigInteger("999999999999999999999999999999999999"), -2)),
            new SchemaAndValueField("VAL_DECIMAL", Schema.OPTIONAL_INT64_SCHEMA, 99999_99999L),
            new SchemaAndValueField("VAL_NUMERIC", Schema.OPTIONAL_INT64_SCHEMA, 99999_99999L),
            new SchemaAndValueField("VAL_NUMBER_1", Schema.OPTIONAL_INT8_SCHEMA, (byte) 1));

    private static final List<SchemaAndValueField> EXPECTED_TIME = Arrays.asList(
            new SchemaAndValueField("VAL_DATE", Timestamp.builder().optional().build(), 1522108800_000l),
            new SchemaAndValueField("VAL_TS", MicroTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890),
            new SchemaAndValueField("VAL_TS_PRECISION2", Timestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130),
            new SchemaAndValueField("VAL_TS_PRECISION4", MicroTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500),
            new SchemaAndValueField("VAL_TS_PRECISION9", NanoTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 125456789),
            new SchemaAndValueField("VAL_TSTZ", ZonedTimestamp.builder().optional().build(), "2018-03-27T01:34:56.007890-11:00"),
            new SchemaAndValueField("VAL_TSLTZ", ZonedTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 1, 34, 56, 7890 * 1_000).atZone(ZoneOffset.systemDefault())
                            .withZoneSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))),
            new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000L),
            new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000L),
            new SchemaAndValueField("VAL_MAX_DATE", Timestamp.builder().optional().build(), 71_863_286_400_000L));

    private static final List<SchemaAndValueField> EXPECTED_TIME_AS_CONNECT = Arrays.asList(
            new SchemaAndValueField("VAL_DATE", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDate.of(2018, 3, 27).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("VAL_TS", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 7890 * 1_000).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("VAL_TS_PRECISION2", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 130 * 1_000_000).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("VAL_TS_PRECISION4", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 125500 * 1_000).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("VAL_TS_PRECISION9", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 125456789).atOffset(ZoneOffset.UTC).toInstant())),
            new SchemaAndValueField("VAL_TSTZ", ZonedTimestamp.builder().optional().build(), "2018-03-27T01:34:56.007890-11:00"),
            new SchemaAndValueField("VAL_TSLTZ", ZonedTimestamp.builder().optional().build(),
                    LocalDateTime.of(2018, 3, 27, 1, 34, 56, 7890 * 1_000).atZone(ZoneOffset.systemDefault())
                            .withZoneSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))),
            new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000L),
            new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000L),
            new SchemaAndValueField("VAL_MAX_DATE", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDate.of(4247, 4, 5).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant())));

    private static final String CLOB_JSON = Testing.Files.readResourceAsString("data/test_lob_data.json");
    private static final String NCLOB_JSON = Testing.Files.readResourceAsString("data/test_lob_data2.json");

    private static final List<SchemaAndValueField> EXPECTED_CLOB = Arrays.asList(
            new SchemaAndValueField("VAL_CLOB_INLINE", Schema.OPTIONAL_STRING_SCHEMA, "TestClob123"),
            new SchemaAndValueField("VAL_NCLOB_INLINE", Schema.OPTIONAL_STRING_SCHEMA, "TestNClob123"),
            new SchemaAndValueField("VAL_CLOB_SHORT", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 0, 512)),
            new SchemaAndValueField("VAL_NCLOB_SHORT", Schema.OPTIONAL_STRING_SCHEMA, part(NCLOB_JSON, 0, 512)),
            new SchemaAndValueField("VAL_CLOB_LONG", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 0, 5000)),
            new SchemaAndValueField("VAL_NCLOB_LONG", Schema.OPTIONAL_STRING_SCHEMA, part(NCLOB_JSON, 0, 5000)));

    private static final List<SchemaAndValueField> EXPECTED_CLOB_UPDATE = Arrays.asList(
            new SchemaAndValueField("VAL_CLOB_INLINE", Schema.OPTIONAL_STRING_SCHEMA, "TestClob123Update"),
            new SchemaAndValueField("VAL_NCLOB_INLINE", Schema.OPTIONAL_STRING_SCHEMA, "TestNClob123Update"),
            new SchemaAndValueField("VAL_CLOB_SHORT", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 1, 512)),
            new SchemaAndValueField("VAL_NCLOB_SHORT", Schema.OPTIONAL_STRING_SCHEMA, part(NCLOB_JSON, 1, 512)),
            new SchemaAndValueField("VAL_CLOB_LONG", Schema.OPTIONAL_STRING_SCHEMA, part(CLOB_JSON, 1, 5000)),
            new SchemaAndValueField("VAL_NCLOB_LONG", Schema.OPTIONAL_STRING_SCHEMA, part(NCLOB_JSON, 1, 5000)));

    private static final List<SchemaAndValueField> EXPECTED_GEOMETRY = Arrays.asList();

    private static final String[] ALL_TABLES = {
            "debezium.type_string",
            "debezium.type_fp",
            "debezium.type_int",
            "debezium.type_time",
            "debezium.type_clob",
            "debezium.type_geometry"
    };

    private static final String[] ALL_DDLS = {
            DDL_STRING,
            DDL_FP,
            DDL_INT,
            DDL_TIME,
            DDL_CLOB,
            DDL_GEOMETRY
    };

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    @Rule
    public final TestRule skipApicurioRule = new SkipTestWhenRunWithApicurioRule();

    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
        dropTables();
    }

    @AfterClass
    public static void dropTables() throws SQLException {
        if (connection != null) {
            for (String table : ALL_TABLES) {
                TestHelper.dropTable(connection, table);
            }
        }
    }

    protected static void createTables() throws SQLException {
        connection.execute(ALL_DDLS);
        for (String table : ALL_TABLES) {
            TestHelper.streamTable(connection, table);
        }
    }

    protected List<String> getAllTables() {
        return Arrays.asList(ALL_TABLES);
    }

    protected abstract boolean insertRecordsDuringTest();

    protected abstract Builder connectorConfig();

    protected abstract void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception;

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
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
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
            VerifyRecord.isValidInsert(record, true);
        }
        else {
            VerifyRecord.isValidRead(record);
        }

        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertRecord(after, EXPECTED_FP_AS_DOUBLE);
    }

    @Test
    @SkipWhenRunWithApicurio
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

    @Test
    @FixFor("DBZ-3268")
    public void timeTypesAsAdaptiveMicroseconds() throws Exception {
        stopConnector();
        init(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);

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

    @Test
    @FixFor("DBZ-3268")
    public void timeTypesAsConnect() throws Exception {
        stopConnector();
        init(TemporalPrecisionMode.CONNECT);

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
        assertRecord(after, EXPECTED_TIME_AS_CONNECT);
    }

    @Test
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void clobTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertClobTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_CLOB");
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
        assertRecord(after, EXPECTED_CLOB);

        if (insertRecordsDuringTest()) {
            // Update clob types
            updateClobTypes();

            records = consumeRecordsByTopic(1);
            testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_CLOB");
            assertThat(testTableRecords).hasSize(1);
            record = testTableRecords.get(0);

            VerifyRecord.isValid(record);
            VerifyRecord.isValidUpdate(record, "ID", 1);

            after = (Struct) ((Struct) record.value()).get("after");
            assertRecord(after, EXPECTED_CLOB_UPDATE);
        }
    }

    @Test
    @FixFor("DBZ-4206")
    public void geometryTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertGeometryTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_GEOMETRY");
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
        // Verify that the SDO_GEOMETRY field is not being emitted as its current unsupported
        assertThat(after.schema().field("LOCATION")).isNull();
        assertRecord(after, EXPECTED_GEOMETRY);
    }

    protected static void insertStringTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_string VALUES (1, 'v\u010d2', 'v\u010d2', 'nv\u010d2', 'c', 'n\u010d')");
        connection.execute("COMMIT");
    }

    protected static void insertFpTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_fp VALUES (1, 1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323, 77.323)");
        connection.execute("COMMIT");
    }

    protected static void insertIntTypes() throws SQLException {
        connection.execute(
                "INSERT INTO debezium.type_int VALUES (1, 1, 22, 333, 4444, 5555, 99, 9999, 999999999, 999999999999999999, 94, 9949, 999999994, 999999999999999949, 99999999999999999999999999999999999949, 9999999999, 9999999999, 1)");
        connection.execute("COMMIT");
    }

    protected static void insertTimeTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_time VALUES ("
                + "1"
                + ", TO_DATE('2018-03-27', 'yyyy-mm-dd')"
                + ", TO_TIMESTAMP('2018-03-27 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP('2018-03-27 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP('2018-03-27 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP('2018-03-27 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9')"
                + ", TO_TIMESTAMP_TZ('2018-03-27 01:34:56.00789 -11:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('2018-03-27 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5')"
                + ", INTERVAL '-3-6' YEAR TO MONTH"
                + ", INTERVAL '-1 2:3:4.56' DAY TO SECOND"
                + ", TO_DATE('4247-04-05', 'yyyy-mm-dd')"
                + ")");
        connection.execute("COMMIT");
    }

    protected static void insertClobTypes() throws SQLException {
        Clob clob1 = connection.connection().createClob();
        clob1.setString(1, part(CLOB_JSON, 0, 512));

        Clob clob2 = connection.connection().createClob();
        clob2.setString(1, part(CLOB_JSON, 0, 5000));

        NClob nclob1 = connection.connection().createNClob();
        nclob1.setString(1, part(NCLOB_JSON, 0, 512));

        NClob nclob2 = connection.connection().createNClob();
        nclob2.setString(1, part(NCLOB_JSON, 0, 5000));

        connection.prepareQuery("INSERT INTO debezium.type_clob VALUES (1, ?, ?, ?, ?, ?, ?)", ps -> {
            ps.setString(1, "TestClob123");
            ps.setString(2, "TestNClob123");
            ps.setClob(3, clob1);
            ps.setNClob(4, nclob1);
            ps.setClob(5, clob2);
            ps.setNClob(6, nclob2);
        }, null);
        connection.commit();
    }

    protected static void updateClobTypes() throws Exception {
        Clob clob1 = connection.connection().createClob();
        clob1.setString(1, part(CLOB_JSON, 1, 512));

        Clob clob2 = connection.connection().createClob();
        clob2.setString(1, part(CLOB_JSON, 1, 5000));

        NClob nclob1 = connection.connection().createNClob();
        nclob1.setString(1, part(NCLOB_JSON, 1, 512));

        NClob nclob2 = connection.connection().createNClob();
        nclob2.setString(1, part(NCLOB_JSON, 1, 5000));

        connection.prepareQuery("UPDATE debezium.type_clob SET VAL_CLOB_INLINE=?, VAL_NCLOB_INLINE=?, VAL_CLOB_SHORT=?, "
                + "VAL_NCLOB_SHORT=?, VAL_CLOB_LONG=?, VAL_NCLOB_LONG=? WHERE ID = 1", ps -> {
                    ps.setString(1, "TestClob123Update");
                    ps.setString(2, "TestNClob123Update");
                    ps.setClob(3, clob1);
                    ps.setNClob(4, nclob1);
                    ps.setClob(5, clob2);
                    ps.setNClob(6, nclob2);
                }, null);
        connection.commit();
    }

    protected static void insertGeometryTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_geometry VALUES ("
                + "1"
                + ", SDO_GEOMETRY(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(1, 1003, 3), SDO_ORDINATE_ARRAY(1, 1, 5, 7))"
                + ")");
    }

    private static String part(String text, int start, int length) {
        return text == null ? "" : text.substring(start, Math.min(length, text.length()));
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

    protected static boolean isHybridMiningStrategy() {
        return OracleConnectorConfig.LogMiningStrategy.HYBRID.equals(TestHelper.logMiningStrategy());
    }
}
