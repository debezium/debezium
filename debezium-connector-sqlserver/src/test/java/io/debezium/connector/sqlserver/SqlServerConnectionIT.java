/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.util.Testing;

/**
 * Integration test for {@link SqlServerConnection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SqlServerConnectionIT {

    private ZoneOffset databaseZoneOffset;

    @Before
    public void before() throws SQLException {
        databaseZoneOffset = getDatabaseZoneOffset();
        TestHelper.dropTestDatabase();
    }

    private ZoneOffset getDatabaseZoneOffset() throws SQLException {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            int datetimeoffset = connection.queryAndMap("SELECT DATEPART(TZoffset, SYSDATETIME())", rs -> {
                rs.next();
                return rs.getInt(1);
            });
            return ZoneOffset.ofTotalSeconds(datetimeoffset * 60);
        }
    }

    @Test
    public void shouldEnableCdcForDatabase() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB1");
            connection.execute("USE testDB1");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB1");
        }
    }

    @Test
    public void shouldEnableCdcWithWrapperFunctionsForTable() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB1");
            connection.execute("USE testDB1");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB1");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'testTable' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testTable (ID int not null identity(1, 1) primary key, NUMBER int, TEXT text)";
            connection.execute(sql);

            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "testTable");
            // insert some data

            connection.execute("INSERT INTO testTable (NUMBER, TEXT) values (1, 'aaa')\n"
                    + "INSERT INTO testTable (NUMBER, TEXT) values (2, 'bbb')");

            // and issue a test call to a CDC wrapper function
            Thread.sleep(5_000); // Need to wait to make sure the min_lsn is available

            // Testing.Print.enable();
            connection.query(
                    "select * from cdc.fn_cdc_get_all_changes_dbo_testTable(sys.fn_cdc_get_min_lsn('dbo_testTable'), sys.fn_cdc_get_max_lsn(), N'all')",
                    rs -> {
                        while (rs.next()) {
                            final BigInteger lsn = new BigInteger(rs.getBytes(1));
                            final StringBuilder sb = new StringBuilder(lsn.toString());
                            for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
                                sb.append(rs.getObject(col)).append(' ');
                            }
                            Testing.print(sb.toString());
                        }
                    });
            Testing.Print.disable();
        }

    }

    @Test
    @FixFor("DBZ-1491")
    public void shouldProperlyGetDefaultColumnValues() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB1");
            connection.execute("USE testDB1");
        }

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB1");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'table_with_defaults' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testDB1.dbo.table_with_defaults ("
                    + "    int_no_default_not_null int not null,"
                    + "    int_no_default int,"
                    + "    bigint_column bigint default (3147483648),"
                    + "    int_column int default (2147483647),"
                    + "    smallint_column smallint default (32767),"
                    + "    tinyint_column tinyint default (255),"
                    + "    bit_column bit default(1),"
                    + "    decimal_column decimal(20,5) default (100.12345),"
                    + "    decimal_mismatch_default numeric(10,5) default 200.1,"
                    + "    numeric_column numeric(10,3) default (200.123),"
                    + "    numeric_mismatch_default numeric(10,3) default 200.1,"
                    + "    money_column money default (922337203685477.58),"
                    + "    money_mismatch_default money default 922337203685477,"
                    + "    smallmoney_column smallmoney default (214748.3647),"
                    + "    smallmoney_mismatch_default smallmoney default 922337203685477,"
                    + "    float_column float default (1.2345e2),"
                    + "    real_column real default (1.2345e3),"
                    + "    date_column date default ('2019-02-03'),"
                    + "    datetime_column datetime default ('2019-01-01 12:34:56.789'),"
                    + "    datetime2_column datetime2 default ('2019-01-01 12:34:56.1234567'),"
                    + "    datetime2_0_column datetime2(0) default ('2019-01-01 12:34:56'),"
                    + "    datetime2_1_column datetime2(1) default ('2019-01-01 12:34:56.1'),"
                    + "    datetime2_2_column datetime2(2) default ('2019-01-01 12:34:56.12'),"
                    + "    datetime2_3_column datetime2(3) default ('2019-01-01 12:34:56.123'),"
                    + "    datetime2_4_column datetime2(4) default ('2019-01-01 12:34:56.1234'),"
                    + "    datetime2_5_column datetime2(5) default ('2019-01-01 12:34:56.12345'),"
                    + "    datetime2_6_column datetime2(6) default ('2019-01-01 12:34:56.123456'),"
                    + "    datetime2_7_column datetime2(7) default ('2019-01-01 12:34:56.1234567'),"
                    + "    datetimeoffset_column datetimeoffset default ('2019-01-01 00:00:00.1234567+02:00'),"
                    + "    smalldatetime_column smalldatetime default ('2019-01-01 12:34:00'),"
                    + "    time_column time default ('12:34:56.1234567'),"
                    + "    time_0_column time(0) default ('12:34:56'),"
                    + "    time_1_column time(1) default ('12:34:56.1'),"
                    + "    time_2_column time(2) default ('12:34:56.12'),"
                    + "    time_3_column time(3) default ('12:34:56.123'),"
                    + "    time_4_column time(4) default ('12:34:56.1234'),"
                    + "    time_5_column time(5) default ('12:34:56.12345'),"
                    + "    time_6_column time(6) default ('12:34:56.123456'),"
                    + "    time_7_column time(7) default ('12:34:56.1234567'),"
                    + "    char_column char(3) default ('aaa'),"
                    + "    varchar_column varchar(20) default ('bbb'),"
                    + "    text_column text default ('ccc'),"
                    + "    nchar_column nchar(3) default ('ddd'),"
                    + "    nvarchar_column nvarchar(20) default ('eee'),"
                    + "    ntext_column ntext default ('fff'),"
                    + "    binary_column binary(5) default (0x0102030405),"
                    + "    varbinary_column varbinary(10) default (0x010203040506),"
                    + "    image_column image default (0x01020304050607)"
                    + ");";

            connection.execute(sql);

            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "table_with_defaults");
            // insert some data

            // and issue a test call to a CDC wrapper function
            Thread.sleep(5_000); // Need to wait to make sure the min_lsn is available
            List<String> capturedColumns = Arrays
                    .asList(
                            "int_no_default_not_null",
                            "int_no_default",
                            "bigint_column",
                            "int_column",
                            "smallint_column",
                            "tinyint_column",
                            "bit_column",
                            "decimal_column",
                            "decimal_mismatch_default",
                            "numeric_column",
                            "numeric_mismatch_default",
                            "money_column",
                            "money_mismatch_default",
                            "smallmoney_column",
                            "smallmoney_mismatch_default",
                            "float_column",
                            "real_column",

                            "date_column",
                            "datetime_column",
                            "datetime2_column",
                            "datetime2_0_column",
                            "datetime2_1_column",
                            "datetime2_2_column",
                            "datetime2_3_column",
                            "datetime2_4_column",
                            "datetime2_5_column",
                            "datetime2_6_column",
                            "datetime2_7_column",
                            "datetimeoffset_column",
                            "smalldatetime_column",

                            "time_column",
                            "time_0_column",
                            "time_1_column",
                            "time_2_column",
                            "time_3_column",
                            "time_4_column",
                            "time_5_column",
                            "time_6_column",
                            "time_7_column",

                            "char_column",
                            "varchar_column",
                            "text_column",
                            "nchar_column",
                            "nvarchar_column",
                            "ntext_column",

                            "binary_column",
                            "varbinary_column",
                            "image_column");

            SqlServerChangeTable changeTable = new SqlServerChangeTable(new TableId("testDB1", "dbo", "table_with_defaults"),
                    null, 0, null, capturedColumns);
            Table table = connection.getTableSchemaFromTable(TestHelper.TEST_DATABASE_1, changeTable);

            TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                    new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null),
                    connection.getDefaultValueConverter(),
                    SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                    FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), true);

            assertColumnHasNotDefaultValue(table, "int_no_default_not_null");
            assertColumnHasDefaultValue(table, "int_no_default", null, tableSchemaBuilder);

            assertColumnHasDefaultValue(table, "bigint_column", 3147483648L, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "int_column", 2147483647, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "smallint_column", (short) 32767, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "tinyint_column", (short) 255, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "bit_column", true, tableSchemaBuilder);
            // The expected BugDecimal must have the correct scale.
            assertColumnHasDefaultValue(table, "decimal_column", new BigDecimal("100.12345"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "decimal_mismatch_default", new BigDecimal("200.10000"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "numeric_column", new BigDecimal("200.123"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "numeric_mismatch_default", new BigDecimal("200.100"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "money_column", new BigDecimal("922337203685477.5800"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "money_mismatch_default", new BigDecimal("922337203685477.0000"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "smallmoney_column", new BigDecimal("214748.3647"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "smallmoney_mismatch_default", new BigDecimal("922337203685477.0000"), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "float_column", 123.45, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "real_column", 1234.5f, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "date_column", 17930, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime_column", toMillis(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 790_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_column", toNanos(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 123_456_700, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_0_column", toMillis(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 0, databaseZoneOffset)), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_1_column", toMillis(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 100_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_2_column", toMillis(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 120_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_3_column", toMillis(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_4_column", toMicros(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 123_400_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_5_column", toMicros(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 123_450_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_6_column", toMicros(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 123_456_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetime2_7_column", toNanos(OffsetDateTime.of(2019, 1, 1, 12, 34, 56, 123_456_700, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "datetimeoffset_column", "2019-01-01T00:00:00.1234567+02:00", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "smalldatetime_column", toMillis(OffsetDateTime.of(2019, 1, 1, 12, 34, 0, 0, databaseZoneOffset)), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_column", toNanos(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_0_column", (int) toMillis(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 0, databaseZoneOffset)), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_1_column", (int) toMillis(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 100_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_2_column", (int) toMillis(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 120_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_3_column", (int) toMillis(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            // JDBC connector does not support full precision for type time(n), n = 4, 5, 6, 7
            assertColumnHasDefaultValue(table, "time_4_column", toMicros(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_5_column", toMicros(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_6_column", toMicros(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "time_7_column", toNanos(OffsetDateTime.of(1970, 1, 1, 12, 34, 56, 123_000_000, databaseZoneOffset)),
                    tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "char_column", "aaa", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "varchar_column", "bbb", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "text_column", "ccc", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nchar_column", "ddd", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nvarchar_column", "eee", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "ntext_column", "fff", tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "binary_column", ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4, 5 }), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "varbinary_column", ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4, 5, 6 }), tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "image_column", ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4, 5, 6, 7 }), tableSchemaBuilder);
        }
    }

    @Test
    @FixFor("DBZ-2698")
    public void shouldProperlyGetDefaultColumnNullValues() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB1");
            connection.execute("USE testDB1");
        }

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB1");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'table_with_defaults' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testDB1.dbo.table_with_defaults ("
                    + "    int_no_default_not_null int not null,"
                    + "    int_no_default int,"
                    + "    int_default_null int default null,"
                    + "    int_column int default (2147483647),"

                    + "    bigint_no_default_not_null bigint not null,"
                    + "    bigint_no_default bigint,"
                    + "    bigint_default_null bigint default null,"
                    + "    bigint_column bigint default (3147483648.),"

                    + "    smallint_no_default_not_null smallint not null,"
                    + "    smallint_no_default smallint,"
                    + "    smallint_default_null smallint default null,"
                    + "    smallint_column smallint default (32767),"

                    + "    tinyint_no_default_not_null tinyint not null,"
                    + "    tinyint_no_default tinyint,"
                    + "    tinyint_default_null tinyint default null,"
                    + "    tinyint_column tinyint default (255),"

                    + "    float_no_default_not_null float not null,"
                    + "    float_no_default float,"
                    + "    float_default_null float default null,"
                    + "    float_column float default (1.2345e2),"

                    + "    real_no_default_not_null real not null,"
                    + "    real_no_default real,"
                    + "    real_default_null real default null,"
                    + "    real_column real default (1.2345e3),"

                    + "    char_default_null_no_paren char(10) default null,"
                    + "    char_default_null_single_paren char(10) default (NULL),"
                    + "    char_default_null_double_paren char(10) default ((NULL)),"
                    + "    varchar_default_null_no_paren varchar(10) default null,"
                    + "    varchar_default_null_single_paren varchar(10) default (NULL),"
                    + "    varchar_default_null_double_paren varchar(10) default ((NULL)),"
                    + "    text_default_null_no_paren text default null,"
                    + "    text_default_null_single_paren text default (NULL),"
                    + "    text_default_null_double_paren text default ((NULL)),"
                    + "    nchar_default_null_no_paren nchar(10) default null,"
                    + "    nchar_default_null_single_paren nchar(10) default (NULL),"
                    + "    nchar_default_null_double_paren nchar(10) default ((NULL)),"
                    + "    nvarchar_default_null_no_paren nvarchar(10) default null,"
                    + "    nvarchar_default_null_single_paren nvarchar(10) default (NULL),"
                    + "    nvarchar_default_null_double_paren nvarchar(10) default ((NULL)),"
                    + "    ntext_default_null_no_paren ntext default null,"
                    + "    ntext_default_null_single_paren ntext default (NULL),"
                    + "    ntext_default_null_double_paren ntext default ((NULL)),"

                    + ");";

            connection.execute(sql);

            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "table_with_defaults");
            // insert some data

            // and issue a test call to a CDC wrapper function
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(() -> connection.getMinLsn(TestHelper.TEST_DATABASE_1, "table_with_defaults").isAvailable()); // Need to wait to make sure the min_lsn is available
            List<String> capturedColumns = Arrays
                    .asList(
                            "int_no_default_not_null",
                            "int_no_default",
                            "int_default_null",
                            "int_column",

                            "bigint_no_default_not_null",
                            "bigint_no_default",
                            "bigint_default_null",
                            "bigint_column",

                            "smallint_no_default_not_null",
                            "smallint_no_default",
                            "smallint_default_null",
                            "smallint_column",

                            "tinyint_no_default_not_null",
                            "tinyint_no_default",
                            "tinyint_default_null",
                            "tinyint_column",

                            "float_no_default_not_null",
                            "float_no_default",
                            "float_default_null",
                            "float_column",

                            "real_no_default_not_null",
                            "real_no_default",
                            "real_default_null",
                            "real_column",

                            "char_default_null_no_paren",
                            "char_default_null_single_paren",
                            "char_default_null_double_paren",
                            "varchar_default_null_no_paren",
                            "varchar_default_null_single_paren",
                            "varchar_default_null_double_paren",
                            "text_default_null_no_paren",
                            "text_default_null_single_paren",
                            "text_default_null_double_paren",
                            "nchar_default_null_no_paren",
                            "nchar_default_null_single_paren",
                            "nchar_default_null_double_paren",
                            "nvarchar_default_null_no_paren",
                            "nvarchar_default_null_single_paren",
                            "nvarchar_default_null_double_paren",
                            "ntext_default_null_no_paren",
                            "ntext_default_null_single_paren",
                            "ntext_default_null_double_paren");

            SqlServerChangeTable changeTable = new SqlServerChangeTable(new TableId("testDB1", "dbo", "table_with_defaults"),
                    null, 0, null, capturedColumns);
            Table table = connection.getTableSchemaFromTable(TestHelper.TEST_DATABASE_1, changeTable);

            TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                    new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null),
                    connection.getDefaultValueConverter(),
                    SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                    FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), true);

            assertColumnHasNotDefaultValue(table, "int_no_default_not_null");
            assertColumnHasDefaultValue(table, "int_no_default", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "int_default_null", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "int_column", 2147483647, tableSchemaBuilder);

            assertColumnHasNotDefaultValue(table, "bigint_no_default_not_null");
            assertColumnHasDefaultValue(table, "bigint_no_default", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "bigint_default_null", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "bigint_column", 3147483648L, tableSchemaBuilder);

            assertColumnHasNotDefaultValue(table, "smallint_no_default_not_null");
            assertColumnHasDefaultValue(table, "smallint_no_default", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "smallint_default_null", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "smallint_column", (short) 32767, tableSchemaBuilder);

            assertColumnHasNotDefaultValue(table, "tinyint_no_default_not_null");
            assertColumnHasDefaultValue(table, "tinyint_no_default", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "tinyint_default_null", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "tinyint_column", (short) 255, tableSchemaBuilder);

            assertColumnHasNotDefaultValue(table, "float_no_default_not_null");
            assertColumnHasDefaultValue(table, "float_no_default", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "float_default_null", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "float_column", 123.45, tableSchemaBuilder);

            assertColumnHasNotDefaultValue(table, "real_no_default_not_null");
            assertColumnHasDefaultValue(table, "real_no_default", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "real_default_null", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "real_column", 1234.5f, tableSchemaBuilder);

            assertColumnHasDefaultValue(table, "char_default_null_no_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "char_default_null_single_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "char_default_null_double_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "varchar_default_null_no_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "varchar_default_null_single_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "varchar_default_null_double_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "text_default_null_no_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "text_default_null_single_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "text_default_null_double_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nchar_default_null_no_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nchar_default_null_single_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nchar_default_null_double_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nvarchar_default_null_no_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nvarchar_default_null_single_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "nvarchar_default_null_double_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "ntext_default_null_no_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "ntext_default_null_single_paren", null, tableSchemaBuilder);
            assertColumnHasDefaultValue(table, "ntext_default_null_double_paren", null, tableSchemaBuilder);
        }
    }

    @Test
    @FixFor("DBZ-4346")
    public void testAccessToCDCTableBasedOnUserRoleAccess() throws Exception {
        // Setup a user with only read-only access
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB1");
            connection.execute("USE testDB1");

            String testUserCreateSql = "IF EXISTS (select 1 from sys.server_principals where name = 'test_user')\n"
                    + "DROP LOGIN test_user\n"
                    + "CREATE LOGIN test_user WITH PASSWORD = 'Password!'\n"
                    + "CREATE USER test_user FOR LOGIN test_user\n"
                    + "ALTER ROLE db_denydatareader ADD MEMBER test_user";

            connection.execute(testUserCreateSql);

            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB1");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects w" +
                    "here name = 'testTable' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testTable (ID int not null identity(1, 1) primary key, NUMBER int, TEXT text)";
            connection.execute(sql);
            // then enable CDC and wrapper functions
            TestHelper.enableTableCdc(connection, "testTable");

            // sa user should have access to CDC table
            assertThat(connection.checkIfConnectedUserHasAccessToCDCTable(TestHelper.TEST_DATABASE_1)).isTrue();
        }

        // Re-connect with the newly created user
        try (SqlServerConnection connection = TestHelper.testConnection("test_user", "Password!")) {
            // This user shouldn't have access to CDC table
            connection.execute("USE testDB1");
            assertThat(connection.checkIfConnectedUserHasAccessToCDCTable(TestHelper.TEST_DATABASE_1)).isFalse();
        }
    }

    @Test
    @FixFor("DBZ-5496")
    public void shouldConnectToASingleDatabase() throws Exception {
        TestHelper.createTestDatabase();
        try (SqlServerConnection connection = TestHelper.testConnection()) {
            assertThat(connection.connection().getCatalog()).isEqualTo(TestHelper.TEST_DATABASE_1);
        }
    }

    @Test
    @FixFor("DBZ-5496")
    public void shouldNotConnectToAnyOfMultipleDatabase() throws Exception {
        TestHelper.createTestDatabases(TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2);
        try (SqlServerConnection connection = TestHelper.multiPartitionTestConnection()) {
            assertThat(connection.connection().getCatalog()).isEqualTo("master");
        }
    }

    @Test
    public void whenQueryTakesMoreThenConfiguredQueryTimeoutAnExceptionMustBeThrown() throws SQLException {

        TestHelper.createTestDatabase();
        Configuration config = TestHelper.defaultConnectorConfig()
                .with("database.query.timeout.ms", "1000").build();

        try (SqlServerConnection conn = TestHelper.testConnection(config)) {
            conn.connect();

            assertThatThrownBy(() -> conn.execute("WAITFOR DELAY '00:01'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("The query has timed out.");
        }
    }

    private long toMillis(OffsetDateTime datetime) {
        return datetime.toInstant().toEpochMilli();
    }

    private long toMicros(OffsetDateTime datetime) {
        Instant instant = datetime.toInstant();
        long seconds = instant.toEpochMilli() / 1000L;
        long micros = instant.getNano() / 1_000L;
        return seconds * 1_000_000L + micros;
    }

    private long toNanos(OffsetDateTime datetime) {
        Instant instant = datetime.toInstant();
        long seconds = instant.toEpochMilli() / 1000L;
        long nanos = instant.getNano();
        return seconds * 1_000_000_000L + nanos;

    }

    private void assertColumnHasNotDefaultValue(Table table, String columnName) {
        Column column = table.columnWithName(columnName);
        assertThat(column.hasDefaultValue()).isFalse();
    }

    private void assertColumnHasDefaultValue(Table table, String columnName, Object expectedValue, TableSchemaBuilder tableSchemaBuilder) {
        Properties properties = new Properties();
        properties.put("topic.prefix", "test");
        TableSchema schema = tableSchemaBuilder.create(new SchemaTopicNamingStrategy(properties, tableSchemaBuilder.isMultiPartitionMode()),
                table, null, null, null);
        Schema columnSchema = schema.getEnvelopeSchema().schema().field("after").schema().field(columnName).schema();

        Column column = table.columnWithName(columnName);
        assertThat(column.hasDefaultValue()).isTrue();
        assertThat(columnSchema.defaultValue()).isEqualTo(expectedValue);
        if (expectedValue instanceof BigDecimal) {
            // safe cast as we know the expectedValue and column.defaultValue are equal
            BigDecimal columnValue = (BigDecimal) columnSchema.defaultValue();
            BigDecimal expectedBigDecimal = (BigDecimal) expectedValue;
            assertThat(column.scale().isPresent()).isTrue();
            int columnScale = column.scale().get();
            assertThat(columnScale).isEqualTo(columnValue.scale());
            assertThat(columnValue.scale()).isEqualTo(expectedBigDecimal.scale());
        }
    }

}
