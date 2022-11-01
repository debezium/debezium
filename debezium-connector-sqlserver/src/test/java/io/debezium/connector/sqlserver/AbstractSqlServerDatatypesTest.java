/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * Integration test to verify different SQL Server datatypes.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSqlServerDatatypesTest extends AbstractConnectorTest {

    /**
     * Key for schema parameter used to store DECIMAL/NUMERIC columns' precision.
     */
    static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    private static final String DDL_STRING = "create table type_string (" +
            "  id int not null, " +
            "  val_char char(3), " +
            "  val_varchar varchar(1000), " +
            "  val_text text, " +
            "  val_nchar nchar(3), " +
            "  val_nvarchar nvarchar(1000), " +
            "  val_ntext ntext, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_FP = "create table type_fp (" +
            "  id int not null, " +
            "  val_decimal decimal(6,3), " +
            "  val_numeric numeric, " +
            "  val_float float, " +
            "  val_real real, " +
            "  val_smallmoney smallmoney, " +
            "  val_money money " +
            "  primary key (id)" +
            ")";

    private static final String DDL_INT = "create table type_int (" +
            "  id int not null, " +
            "  val_bit bit, " +
            "  val_tinyint tinyint, " +
            "  val_smallint smallint, " +
            "  val_int int, " +
            "  val_bigint bigint, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_TIME = "create table type_time (" +
            "  id int not null, " +
            "  val_date date, " +
            "  val_time_p2 time(2), " +
            "  val_time time(4), " +
            "  val_datetime2 datetime2, " +
            "  val_datetimeoffset datetimeoffset, " +
            "  val_datetime datetime, " +
            "  val_smalldatetime smalldatetime, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_XML = "create table type_xml (" +
            "  id int not null, " +
            "  val_xml xml, " +
            "  primary key (id)" +
            ")";

    private static final List<SchemaAndValueField> EXPECTED_INT = Arrays.asList(
            new SchemaAndValueField("val_bit", Schema.OPTIONAL_BOOLEAN_SCHEMA, true),
            new SchemaAndValueField("val_tinyint", Schema.OPTIONAL_INT16_SCHEMA, (short) 22),
            new SchemaAndValueField("val_smallint", Schema.OPTIONAL_INT16_SCHEMA, (short) 333),
            new SchemaAndValueField("val_int", Schema.OPTIONAL_INT32_SCHEMA, 4444),
            new SchemaAndValueField("val_bigint", Schema.OPTIONAL_INT64_SCHEMA, 55555l));

    private static final List<SchemaAndValueField> EXPECTED_FP = Arrays.asList(
            new SchemaAndValueField("val_decimal", Decimal.builder(3).parameter(PRECISION_PARAMETER_KEY, "6").optional().build(), new BigDecimal("1.123")),
            new SchemaAndValueField("val_numeric", Decimal.builder(0).parameter(PRECISION_PARAMETER_KEY, "18").optional().build(), new BigDecimal("2")),
            new SchemaAndValueField("val_float", Schema.OPTIONAL_FLOAT64_SCHEMA, 3.323),
            new SchemaAndValueField("val_real", Schema.OPTIONAL_FLOAT32_SCHEMA, 4.323f),
            new SchemaAndValueField("val_smallmoney", Decimal.builder(4).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("5.3230")),
            new SchemaAndValueField("val_money", Decimal.builder(4).parameter(PRECISION_PARAMETER_KEY, "19").optional().build(), new BigDecimal("6.3230")));

    private static final List<SchemaAndValueField> EXPECTED_STRING = Arrays.asList(
            new SchemaAndValueField("val_char", Schema.OPTIONAL_STRING_SCHEMA, "cc "),
            new SchemaAndValueField("val_varchar", Schema.OPTIONAL_STRING_SCHEMA, "vcc"),
            new SchemaAndValueField("val_text", Schema.OPTIONAL_STRING_SCHEMA, "tc"),
            new SchemaAndValueField("val_nchar", Schema.OPTIONAL_STRING_SCHEMA, "c\u010d "),
            new SchemaAndValueField("val_nvarchar", Schema.OPTIONAL_STRING_SCHEMA, "vc\u010d"),
            new SchemaAndValueField("val_ntext", Schema.OPTIONAL_STRING_SCHEMA, "t\u010d"));

    private static final List<SchemaAndValueField> EXPECTED_DATE_TIME = Arrays.asList(
            new SchemaAndValueField("val_date", Date.builder().optional().build(), 17_725),
            new SchemaAndValueField("val_time_p2", Time.builder().optional().build(), 37_425_680),
            new SchemaAndValueField("val_time", MicroTime.builder().optional().build(), 37_425_678_900L),
            new SchemaAndValueField("val_datetime2", NanoTimestamp.builder().optional().build(), 1_531_481_025_340_000_000L),
            new SchemaAndValueField("val_datetimeoffset", ZonedTimestamp.builder().optional().build(), "2018-07-13T12:23:45.456+11:00"),
            new SchemaAndValueField("val_datetime", Timestamp.builder().optional().build(), 1_531_488_225_780L),
            new SchemaAndValueField("val_smalldatetime", Timestamp.builder().optional().build(), 1_531_491_840_000L));

    private static final List<SchemaAndValueField> EXPECTED_DATE_TIME_AS_CONNECT = Arrays.asList(
            new SchemaAndValueField("val_date", org.apache.kafka.connect.data.Date.builder().optional().build(),
                    java.util.Date.from(LocalDate.of(2018, 7, 13).atStartOfDay()
                            .atOffset(ZoneOffset.UTC)
                            .toInstant())),
            new SchemaAndValueField("val_time_p2", org.apache.kafka.connect.data.Time.builder().optional().build(),
                    java.util.Date.from(LocalTime.of(10, 23, 45, 680_000_000).atDate(LocalDate.ofEpochDay(0))
                            .atOffset(ZoneOffset.UTC)
                            .toInstant())),
            new SchemaAndValueField("val_time", org.apache.kafka.connect.data.Time.builder().optional().build(),
                    java.util.Date.from(LocalTime.of(10, 23, 45, 678_900_000).atDate(LocalDate.ofEpochDay(0))
                            .atOffset(ZoneOffset.UTC)
                            .toInstant())),
            new SchemaAndValueField("val_datetime2", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 7, 13, 11, 23, 45, 340_000_000)
                            .atOffset(ZoneOffset.UTC)
                            .toInstant())),
            new SchemaAndValueField("val_datetimeoffset", ZonedTimestamp.builder().optional().build(), "2018-07-13T12:23:45.456+11:00"),
            new SchemaAndValueField("val_datetime", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 7, 13, 13, 23, 45, 780_000_000)
                            .atOffset(ZoneOffset.UTC)
                            .toInstant())),
            new SchemaAndValueField("val_smalldatetime", org.apache.kafka.connect.data.Timestamp.builder().optional().build(),
                    java.util.Date.from(LocalDateTime.of(2018, 7, 13, 14, 24, 00)
                            .atOffset(ZoneOffset.UTC)
                            .toInstant())));

    private static final List<SchemaAndValueField> EXPECTED_XML = Arrays.asList(
            new SchemaAndValueField("val_xml", Schema.OPTIONAL_STRING_SCHEMA, "<a>b</a>"));

    private static final String[] ALL_TABLES = {
            "type_int",
            "type_fp",
            "type_string",
            "type_time",
            "type_xml"
    };

    private static final String[] ALL_DDLS = {
            DDL_INT,
            DDL_FP,
            DDL_STRING,
            DDL_TIME,
            DDL_XML
    };

    private static final int EXPECTED_RECORD_COUNT = ALL_DDLS.length;

    @AfterClass
    public static void dropTables() throws SQLException {
        TestHelper.dropTestDatabase();
    }

    @BeforeClass
    public static void createTables() throws SQLException {
        TestHelper.createTestDatabase();
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.execute(ALL_DDLS);
            for (String table : ALL_TABLES) {
                TestHelper.enableTableCdc(connection, table);
            }
            connection.execute(
                    "INSERT INTO type_int VALUES (0, 1, 22, 333, 4444, 55555)",
                    "INSERT INTO type_fp VALUES (0, 1.123, 2, 3.323, 4.323, 5.323, 6.323)",
                    "INSERT INTO type_string VALUES (0, 'c\u010d', 'vc\u010d', 't\u010d', N'c\u010d', N'vc\u010d', N't\u010d')",
                    "INSERT INTO type_time VALUES (0, '2018-07-13', '10:23:45.678', '10:23:45.6789', '2018-07-13 11:23:45.34', '2018-07-13 12:23:45.456+11:00', '2018-07-13 13:23:45.78', '2018-07-13 14:23:45')",
                    "INSERT INTO type_xml VALUES (0, '<a>b</a>')");

            // Make sure to wait for the CDC record for the last insert.
            TestHelper.waitForCdcRecord(connection, "type_xml", rs -> rs.getInt("id") == 0);
        }
    }

    @Test
    public void intTypes() throws Exception {
        Testing.debug("Inserted");

        final SourceRecords records = consumeRecordsByTopic(EXPECTED_RECORD_COUNT);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.testDB1.dbo.type_int");
        assertThat(testTableRecords).hasSize(1);

        // insert
        VerifyRecord.isValidRead(testTableRecords.get(0));
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_INT);
    }

    @Test
    public void fpTypes() throws Exception {
        Testing.debug("Inserted");

        final SourceRecords records = consumeRecordsByTopic(EXPECTED_RECORD_COUNT);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.testDB1.dbo.type_fp");
        assertThat(testTableRecords).hasSize(1);

        // insert
        VerifyRecord.isValidRead(testTableRecords.get(0));
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_FP);
    }

    @Test
    public void stringTypes() throws Exception {
        Testing.debug("Inserted");

        final SourceRecords records = consumeRecordsByTopic(EXPECTED_RECORD_COUNT);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.testDB1.dbo.type_string");
        assertThat(testTableRecords).hasSize(1);

        // insert
        VerifyRecord.isValidRead(testTableRecords.get(0));
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_STRING);
    }

    @Test
    public void dateTimeTypes() throws Exception {
        Testing.debug("Inserted");

        final SourceRecords records = consumeRecordsByTopic(EXPECTED_RECORD_COUNT);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.testDB1.dbo.type_time");
        assertThat(testTableRecords).hasSize(1);

        // insert
        VerifyRecord.isValidRead(testTableRecords.get(0));
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_DATE_TIME);
    }

    @Test
    public void dateTimeTypesAsConnect() throws Exception {
        stopConnector();
        init(TemporalPrecisionMode.CONNECT);

        Testing.debug("Inserted");

        final SourceRecords records = consumeRecordsByTopic(EXPECTED_RECORD_COUNT);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.testDB1.dbo.type_time");
        assertThat(testTableRecords).hasSize(1);

        // insert
        VerifyRecord.isValidRead(testTableRecords.get(0));
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_DATE_TIME_AS_CONNECT);
    }

    @Test
    public void otherTypes() throws Exception {
        Testing.debug("Inserted");

        final SourceRecords records = consumeRecordsByTopic(EXPECTED_RECORD_COUNT);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.testDB1.dbo.type_xml");
        assertThat(testTableRecords).hasSize(1);

        // insert
        VerifyRecord.isValidRead(testTableRecords.get(0));
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_XML);
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }

    public void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception {
        initializeConnectorTestFramework();
        Testing.Debug.enable();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode)
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForSnapshotToBeCompleted();
    }
}
