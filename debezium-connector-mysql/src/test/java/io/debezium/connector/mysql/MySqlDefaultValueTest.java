/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.AbstractDdlParser;
import io.debezium.time.ZonedTimestamp;

/**
 * @author laomei
 */
public class MySqlDefaultValueTest {

    protected AbstractDdlParser parser;
    protected Tables tables;
    private MySqlValueConverters converters;

    @Before
    public void beforeEach() {
        converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES);
        parser = new MySqlAntlrDdlParser(converters);
        tables = new Tables();
    }

    @Test
    public void parseUnsignedTinyintDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_TINYINT_TABLE (" +
                "    A TINYINT UNSIGNED NULL DEFAULT 0," +
                "    B TINYINT UNSIGNED NULL DEFAULT '10'," +
                "    C TINYINT UNSIGNED NULL," +
                "    D TINYINT UNSIGNED NOT NULL," +
                "    E TINYINT UNSIGNED NOT NULL DEFAULT 0," +
                "    F TINYINT UNSIGNED NOT NULL DEFAULT '0'," +
                "    G TINYINT UNSIGNED NOT NULL DEFAULT '255'" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_TINYINT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo((short) 0);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo((short) 10);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("C").defaultValue()).isNull();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo((short) 0);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo((short) 0);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo((short) 255);
    }

    @Test
    public void parseUnsignedSmallintDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_SMALLINT_TABLE (\n" +
                "  A SMALLINT UNSIGNED NULL DEFAULT 0,\n" +
                "  B SMALLINT UNSIGNED NULL DEFAULT '10',\n" +
                "  C SMALLINT UNSIGNED NULL,\n" +
                "  D SMALLINT UNSIGNED NOT NULL,\n" +
                "  E SMALLINT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F SMALLINT UNSIGNED NOT NULL DEFAULT '0',\n" +
                "  G SMALLINT UNSIGNED NOT NULL DEFAULT '65535'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_SMALLINT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(10);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(65535);
    }

    @Test
    public void parseUnsignedMediumintDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_MEDIUMINT_TABLE (\n" +
                "  A MEDIUMINT UNSIGNED NULL DEFAULT 0,\n" +
                "  B MEDIUMINT UNSIGNED NULL DEFAULT '10',\n" +
                "  C MEDIUMINT UNSIGNED NULL,\n" +
                "  D MEDIUMINT UNSIGNED NOT NULL,\n" +
                "  E MEDIUMINT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F MEDIUMINT UNSIGNED NOT NULL DEFAULT '0',\n" +
                "  G MEDIUMINT UNSIGNED NOT NULL DEFAULT '16777215'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_MEDIUMINT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(10);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(16777215);
    }

    @Test
    public void parseUnsignedIntDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_INT_TABLE (\n" +
                "  A INT UNSIGNED NULL DEFAULT 0,\n" +
                "  B INT UNSIGNED NULL DEFAULT '10',\n" +
                "  C INT UNSIGNED NULL,\n" +
                "  D INT UNSIGNED NOT NULL,\n" +
                "  E INT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F INT UNSIGNED NOT NULL DEFAULT '0',\n" +
                "  G INT UNSIGNED NOT NULL DEFAULT '4294967295'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_INT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(0L);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(10L);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(0L);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(0L);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(4294967295L);
    }

    @Test
    public void parseUnsignedBigIntDefaultValueToLong() {
        String sql = "CREATE TABLE UNSIGNED_BIGINT_TABLE (\n" +
                "  A BIGINT UNSIGNED NULL DEFAULT 0,\n" +
                "  B BIGINT UNSIGNED NULL DEFAULT '10',\n" +
                "  C BIGINT UNSIGNED NULL,\n" +
                "  D BIGINT UNSIGNED NOT NULL,\n" +
                "  E BIGINT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F BIGINT UNSIGNED NOT NULL DEFAULT '0'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_BIGINT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(0L);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(10L);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(0L);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(0L);
    }

    @Test
    public void parseUnsignedBigIntDefaultValueToBigDecimal() {
        final MySqlValueConverters converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                BinaryHandlingMode.BYTES);
        final AbstractDdlParser parser = new MySqlAntlrDdlParser(converters);
        String sql = "CREATE TABLE UNSIGNED_BIGINT_TABLE (\n" +
                "  A BIGINT UNSIGNED NULL DEFAULT 0,\n" +
                "  B BIGINT UNSIGNED NULL DEFAULT '10',\n" +
                "  C BIGINT UNSIGNED NULL,\n" +
                "  D BIGINT UNSIGNED NOT NULL,\n" +
                "  E BIGINT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F BIGINT UNSIGNED NOT NULL DEFAULT '0',\n" +
                "  G BIGINT UNSIGNED NOT NULL DEFAULT '18446744073709551615'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_BIGINT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(new BigDecimal(10));
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(new BigDecimal("18446744073709551615"));
    }

    @Test
    public void parseStringDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_STRING_TABLE (\n" +
                "  A CHAR NULL DEFAULT 'A',\n" +
                "  B CHAR CHARACTER SET utf8 NULL DEFAULT 'b',\n" +
                "  C VARCHAR(10) NULL DEFAULT 'CC',\n" +
                "  D NCHAR(10) NULL DEFAULT '10',\n" +
                "  E NVARCHAR NULL DEFAULT '0',\n" +
                "  F CHAR DEFAULT NULL,\n" +
                "  G VARCHAR(10) DEFAULT NULL,\n" +
                "  H NCHAR(10) DEFAULT NULL\n" +
                ") CHARACTER SET 'latin2';";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_STRING_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo("A");
        assertThat(table.columnWithName("A").charsetName()).isEqualTo("latin2");
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo("b");
        assertThat(table.columnWithName("B").charsetName()).isEqualTo("utf8");
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo("CC");
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo("10");
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo("0");
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("H").defaultValue()).isEqualTo(null);
    }

    @Test
    public void parseBitDefaultValue() {
        String sql = "CREATE TABLE BIT_TABLE (\n" +
                "  A BIT(1) NULL DEFAULT NULL,\n" +
                "  B BIT(1) DEFAULT 0,\n" +
                "  C BIT(1) DEFAULT 1,\n" +
                "  D BIT(1) DEFAULT b'0',\n" +
                "  E BIT(1) DEFAULT b'1',\n" +
                "  F BIT(1) DEFAULT TRUE,\n" +
                "  G BIT(1) DEFAULT FALSE,\n" +
                "  H BIT(10) DEFAULT b'101000010',\n" +
                "  I BIT(10) DEFAULT NULL,\n" +
                "  J BIT(25) DEFAULT b'10110000100001111'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "BIT_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(false);
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(false);
        assertThat(table.columnWithName("H").defaultValue()).isEqualTo(new byte[]{ 66, 1 });
        assertThat(table.columnWithName("I").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("J").defaultValue()).isEqualTo(new byte[]{ 15, 97, 1, 0 });
    }

    @Test
    public void parseBooleanDefaultValue() {
        String sql = "CREATE TABLE BOOLEAN_TABLE (\n" +
                "  A BOOLEAN NULL DEFAULT 0,\n" +
                "  B BOOLEAN NOT NULL DEFAULT '1',\n" +
                "  C BOOLEAN NOT NULL DEFAULT '9',\n" +
                "  D BOOLEAN NOT NULL DEFAULT TRUE,\n" +
                "  E BOOLEAN DEFAULT NULL\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "BOOLEAN_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(false);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(null);
    }

    @Test
    public void parseNumberDefaultValue() {
        String sql = "CREATE TABLE NUMBER_TABLE (\n" +
                "  A TINYINT NULL DEFAULT 10,\n" +
                "  B SMALLINT NOT NULL DEFAULT '5',\n" +
                "  C INTEGER NOT NULL DEFAULT 0,\n" +
                "  D BIGINT NOT NULL DEFAULT 20,\n" +
                "  E INT NULL DEFAULT NULL,\n" +
                "  F FLOAT NULL DEFAULT 0,\n" +
                "  G DOUBLE NOT NULL DEFAULT 1.0\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMBER_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo((short) 10);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo((short) 5);
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(0);
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo(20L);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(0d);
    }

    @Test
    public void parseRealDefaultValue() {
        String sql = "CREATE TABLE REAL_TABLE (\n" +
                "  A REAL NOT NULL DEFAULT 1,\n" +
                "  B REAL NULL DEFAULT NULL \n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "REAL_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(1f);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(null);
    }

    @Test
    public void parseNumericAndDecimalToDoubleDefaultValue() {
        String sql = "CREATE TABLE NUMERIC_DECIMAL_TABLE (\n" +
                "  A NUMERIC NOT NULL DEFAULT 1.23,\n" +
                "  B DECIMAL(5,3) NOT NULL DEFAULT 2.321,\n" +
                "  C NUMERIC NULL DEFAULT '12.678',\n" +
                "  D DECIMAL(5,2) NULL DEFAULT '12.678'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMERIC_DECIMAL_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(1.0d);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(2.321d);
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(13d);
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo(12.68d);
    }

    @Test
    public void parseNumericAndDecimalToDecimalDefaultValue() {
        final MySqlValueConverters converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES);
        final AbstractDdlParser parser = new MySqlAntlrDdlParser(converters);
        String sql = "CREATE TABLE NUMERIC_DECIMAL_TABLE (\n" +
                "  A NUMERIC NOT NULL DEFAULT 1.23,\n" +
                "  B DECIMAL(5,3) NOT NULL DEFAULT 2.321,\n" +
                "  C NUMERIC NULL DEFAULT '12.678'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMERIC_DECIMAL_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(BigDecimal.valueOf(1));
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(BigDecimal.valueOf(2.321));
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(BigDecimal.valueOf(13));
    }

    @Test
    public void parseTimeDefaultValue() {
        String sql = "CREATE TABLE TIME_TABLE (" +
                "  A timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                "  B timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'," +
                "  C timestamp NOT NULL DEFAULT '0000-00-00 00:00:00.000'," +
                "  D timestamp NOT NULL DEFAULT '2018-06-26 12:34:56'," +
                "  E timestamp NOT NULL DEFAULT '2018-06-26 12:34:56.000'," +
                "  F timestamp NOT NULL DEFAULT '2018-06-26 12:34:56.78'," +
                "  G datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                "  H datetime NOT NULL DEFAULT '0000-00-00 00:00:00'," +
                "  I datetime(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000'," +
                "  J datetime NOT NULL DEFAULT '2018-06-26 12:34:56'," +
                "  K datetime(3) NOT NULL DEFAULT '2018-06-26 12:34:56.000'," +
                "  L datetime(2) NOT NULL DEFAULT '2018-06-26 12:34:56.78'," +
                "  M datetime NOT NULL DEFAULT '2000-01-00 00:00:00'," +
                "  N datetime NOT NULL DEFAULT '0000-12-01 00:00:00'," +
                "  O datetime NOT NULL DEFAULT '2000-00-01 00:00:00'" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "TIME_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(table.columnWithName("D").defaultValue())
                .isEqualTo(ZonedTimestamp.toIsoString(LocalDateTime.of(2018, 6, 26, 12, 34, 56, 0).atZone(ZoneId.systemDefault()), null));
        assertThat(table.columnWithName("E").defaultValue())
                .isEqualTo(ZonedTimestamp.toIsoString(LocalDateTime.of(2018, 6, 26, 12, 34, 56, 0).atZone(ZoneId.systemDefault()), null));
        assertThat(table.columnWithName("F").defaultValue())
                .isEqualTo(ZonedTimestamp.toIsoString(LocalDateTime.of(2018, 6, 26, 12, 34, 56, 780_000_000).atZone(ZoneId.systemDefault()), null));
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(Date.from(Instant.ofEpochMilli(0)));
        assertThat(table.columnWithName("H").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("I").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("J").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 6, 26, 12, 34, 56, 0, ZoneOffset.UTC).toInstant()));
        assertThat(table.columnWithName("K").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 6, 26, 12, 34, 56, 0, ZoneOffset.UTC).toInstant()));
        assertThat(table.columnWithName("L").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 6, 26, 12, 34, 56, 780_000_000, ZoneOffset.UTC).toInstant()));
        assertThat(table.columnWithName("M").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("N").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("O").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
    }

    @Test
    public void parseDateDefaultValue() {
        String sql = "CREATE TABLE DATE_TABLE (" +
                "  A date NOT NULL DEFAULT '0000-00-00'," +
                "  B date NOT NULL DEFAULT '2018-00-01'," +
                "  C date NOT NULL DEFAULT '0000-12-31'," +
                "  D date NOT NULL DEFAULT '2018-01-00'," +
                "  E date NOT NULL DEFAULT '9999-09-09'," +
                "  F date NOT NULL DEFAULT '1111-11-11'," +
                "  G date NOT NULL DEFAULT '2018-08-31'," +
                "  H date NOT NULL DEFAULT 0" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "DATE_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(9999, 9, 9, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(1111, 11, 11, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 8, 31, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(table.columnWithName("H").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
    }

    @Test
    @FixFor("DBZ-901")
    public void parseAlterTableTruncatedDefaulDateTime() {
        String sql = "CREATE TABLE TIME_TABLE (" +
                "  A datetime(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000'" +
                ");";
        String alterSql = "ALTER TABLE TIME_TABLE ADD COLUMN B DATETIME(3) NOT NULL DEFAULT '1970-01-01 00:00:00';";
        parser.parse(sql, tables);
        parser.parse(alterSql, tables);
        Table table = tables.forTable(new TableId(null, null, "TIME_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
    }

    @Test
    @FixFor("DBZ-870")
    public void shouldAcceptZeroAsDefaultValueForDateColumn() {
        String ddl = "CREATE TABLE data(id INT, nullable_date date default 0, not_nullable_date date not null default 0, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat(table.columnWithName("nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to null for nullable column
        assertThat(table.columnWithName("nullable_date").defaultValue()).isNull();

        assertThat(table.columnWithName("not_nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to epoch for non-nullable column (expecting Date, as this test is using "connect"
        // mode)
        assertThat(table.columnWithName("not_nullable_date").defaultValue()).isEqualTo(getEpochDate());
    }

    private Date getEpochDate() {
        return Date.from(LocalDate.of(1970, 1, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    }

    @Test
    @FixFor("DBZ-1204")
    public void shouldAcceptBooleanAsTinyIntDefaultValue() {
        String ddl = "CREATE TABLE data(id INT, "
                + "bval BOOLEAN DEFAULT TRUE, "
                + "tival1 TINYINT(1) DEFAULT FALSE, "
                + "tival2 TINYINT(1) DEFAULT 3, "
                + "tival3 TINYINT(2) DEFAULT TRUE, "
                + "tival4 TINYINT(2) DEFAULT 18, "
                + "PRIMARY KEY (id))";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat((Boolean) table.columnWithName("bval").defaultValue()).isTrue();
        assertThat((Short) table.columnWithName("tival1").defaultValue()).isZero();
        assertThat((Short) table.columnWithName("tival2").defaultValue()).isEqualTo((short) 3);
        assertThat((Short) table.columnWithName("tival3").defaultValue()).isEqualTo((short) 1);
        assertThat((Short) table.columnWithName("tival4").defaultValue()).isEqualTo((short) 18);
    }

    @Test
    @FixFor("DBZ-1689")
    public void shouldAcceptBooleanAsIntDefaultValue() {
        String ddl = "CREATE TABLE data(id INT, "
                + "bval BOOLEAN DEFAULT TRUE, "
                + "ival1 INT(1) DEFAULT FALSE, "
                + "ival2 INT(1) DEFAULT 3, "
                + "ival3 INT(2) DEFAULT TRUE, "
                + "ival4 INT(2) DEFAULT 18, "
                + "PRIMARY KEY (id))";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat((Boolean) table.columnWithName("bval").defaultValue()).isTrue();
        assertThat((Integer) table.columnWithName("ival1").defaultValue()).isZero();
        assertThat((Integer) table.columnWithName("ival2").defaultValue()).isEqualTo((int) 3);
        assertThat((Integer) table.columnWithName("ival3").defaultValue()).isEqualTo((int) 1);
        assertThat((Integer) table.columnWithName("ival4").defaultValue()).isEqualTo((int) 18);
    }

    @Test
    @FixFor("DBZ-1249")
    public void shouldAcceptBitSetDefaultValue() {
        String ddl = "CREATE TABLE user_subscribe (id bigint(20) unsigned NOT NULL AUTO_INCREMENT, content bit(24) DEFAULT b'111111111111101100001110', PRIMARY KEY (id)) ENGINE=InnoDB";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "user_subscribe"));

        final byte[] defVal = (byte[]) table.columnWithName("content").defaultValue();
        assertThat(Byte.toUnsignedInt((defVal[0]))).isEqualTo(0b00001110);
        assertThat(Byte.toUnsignedInt((defVal[1]))).isEqualTo(0b11111011);
        assertThat(Byte.toUnsignedInt((defVal[2]))).isEqualTo(0b11111111);
    }
}
