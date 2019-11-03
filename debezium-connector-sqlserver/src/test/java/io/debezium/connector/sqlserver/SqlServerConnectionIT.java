/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import microsoft.sql.DateTimeOffset;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;

/**
 * Integration test for {@link SqlServerConnection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SqlServerConnectionIT {

    @Before
    public void before() throws SQLException {
        TestHelper.dropTestDatabase();
    }

    @Test
    public void shouldEnableCdcForDatabase() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");
        }
    }

    @Test
    public void shouldEnableCdcWithWrapperFunctionsForTable() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");

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

            Testing.Print.enable();
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
    public void shouldProperlyGetDefaultColumnValues() throws Exception {
        try (SqlServerConnection connection = TestHelper.adminConnection()) {
            connection.connect();
            connection.execute("CREATE DATABASE testDB");
            connection.execute("USE testDB");
        }

        try (SqlServerConnection connection = TestHelper.testConnection()) {
            connection.connect();
            // NOTE: you cannot enable CDC on master
            TestHelper.enableDbCdc(connection, "testDB");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'table_with_defaults' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testDB.dbo.table_with_defaults ("
                    + "    int_no_default_not_null int not null,"
                    + "    int_no_default int,"
                    + "    bigint_column bigint default (3147483648),"
                    + "    int_column int default (2147483647),"
                    + "    smallint_column smallint default (32767),"
                    + "    tinyint_column tinyint default (255),"
                    + "    bit_column bit default(1),"
                    + "    decimal_column decimal(20,5) default (100.12345),"
                    + "    numeric_column numeric(10,3) default (200.123),"
                    + "    money_column money default (922337203685477.58),"
                    + "    smallmoney_column smallmoney default (214748.3647),"
                    + "    float_column float default (1.2345e2),"
                    + "    real_column real default (1.2345e3),"
                    + "    date_column date default ('2019-02-03'),"
                    + "    datetime_column datetime default ('2019-01-01 00:00:00.000'),"
                    + "    datetime2_column datetime2 default ('2019-01-01 00:00:00.1234567'),"
                    + "    datetimeoffset_column datetimeoffset default ('2019-01-01 00:00:00.1234567+02:00'),"
                    + "    smalldatetime_column smalldatetime default ('2019-01-01 00:00:00'),"
                    + "    time_column time default ('00:00:56.123'),"
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

            ChangeTable changeTable = new ChangeTable(new TableId("testDB", "dbo", "table_with_defaults"),
                    null, 0, null, null);
            Table table = connection.getTableSchemaFromTable(changeTable);

            assertColumnHasNotDefaultValue(table, "int_no_default_not_null");
            assertColumnHasDefaultValue(table, "int_no_default", null);

            assertColumnHasDefaultValue(table, "bigint_column", 3147483648L);
            assertColumnHasDefaultValue(table, "int_column", 2147483647);
            assertColumnHasDefaultValue(table, "smallint_column", (short) 32767);
            assertColumnHasDefaultValue(table, "tinyint_column", (short) 255);
            assertColumnHasDefaultValue(table, "bit_column", true);
            assertColumnHasDefaultValue(table, "decimal_column", new BigDecimal("100.12345"));
            assertColumnHasDefaultValue(table, "numeric_column", new BigDecimal("200.123"));
            assertColumnHasDefaultValue(table, "money_column", new BigDecimal("922337203685477.58"));
            assertColumnHasDefaultValue(table, "smallmoney_column", new BigDecimal("214748.3647"));
            assertColumnHasDefaultValue(table, "float_column", 123.45);
            assertColumnHasDefaultValue(table, "real_column", 1234.5f);
            assertColumnHasDefaultValue(table, "date_column", Date.valueOf("2019-02-03"));
            assertColumnHasDefaultValue(table, "datetime_column", Timestamp.valueOf("2019-01-01 00:00:00.000"));
            assertColumnHasDefaultValue(table, "datetime2_column", Timestamp.valueOf("2019-01-01 00:00:00.1234567"));
            assertColumnHasDefaultValue(table, "datetimeoffset_column", nanosToDatetimeoffset(1546293600123456700L, 120));
            assertColumnHasDefaultValue(table, "smalldatetime_column", Timestamp.valueOf("2019-01-01 00:00:00"));
            // JDBC connector provides accuracy limited to milliseconds only.
            assertColumnHasDefaultValue(table, "time_column", new Time(56123));
            assertColumnHasDefaultValue(table, "char_column", "aaa");
            assertColumnHasDefaultValue(table, "varchar_column", "bbb");
            assertColumnHasDefaultValue(table, "text_column", "ccc");
            assertColumnHasDefaultValue(table, "nchar_column", "ddd");
            assertColumnHasDefaultValue(table, "nvarchar_column", "eee");
            assertColumnHasDefaultValue(table, "ntext_column", "fff");
            assertColumnHasDefaultValue(table, "binary_column", new byte[]{ 1, 2, 3, 4, 5 });
            assertColumnHasDefaultValue(table, "varbinary_column", new byte[]{ 1, 2, 3, 4, 5, 6 });
            assertColumnHasDefaultValue(table, "image_column", new byte[]{ 1, 2, 3, 4, 5, 6, 7 });
        }
    }

    private DateTimeOffset nanosToDatetimeoffset(long nanos, int offset) {
        Timestamp dateTimeOffsetPart = new Timestamp(nanos / 1_000_000);
        dateTimeOffsetPart.setNanos((int) (nanos % 1_000_000_000L));
        return DateTimeOffset.valueOf(dateTimeOffsetPart, offset);
    }

    private void assertColumnHasNotDefaultValue(Table table, String columnName) {
        Column column = table.columnWithName(columnName);
        Assertions.assertThat(column.hasDefaultValue()).isFalse();
    }

    private void assertColumnHasDefaultValue(Table table, String columnName, Object expectedValue) {
        Column column = table.columnWithName(columnName);
        Assertions.assertThat(column.hasDefaultValue()).isTrue();
        Assertions.assertThat(column.defaultValue()).isEqualTo(expectedValue);
    }

}
