/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author laomei
 */
public class MysqlDefaultValueTest {

    private MySqlDdlParser parser;
    private Tables tables;
    private SimpleDdlParserListener listener;
    private MySqlValueConverters converters;

    @Before
    public void beforeEach() {
        converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                                              TemporalPrecisionMode.CONNECT,
                                              JdbcValueConverters.BigIntUnsignedMode.LONG);
        parser = new MySqlDdlParser(false, converters);
        tables = new Tables();
        listener = new SimpleDdlParserListener();
        parser.addListener(listener);
    }

    @Test
    public void parseUnsignedTinyintDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_TINYINT_TABLE (" +
                "    A TINYINT UNSIGNED NULL DEFAULT 0," +
                "    B TINYINT UNSIGNED NULL DEFAULT '10'," +
                "    C TINYINT UNSIGNED NULL," +
                "    D TINYINT UNSIGNED NOT NULL," +
                "    E TINYINT UNSIGNED NOT NULL DEFAULT 0," +
                "    F TINYINT UNSIGNED NOT NULL DEFAULT '0'" +
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
    }

    @Test
    public void parseUnsignedSmallintDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_SMALLINT_TABLE (\n" +
                "  A SMALLINT UNSIGNED NULL DEFAULT 0,\n" +
                "  B SMALLINT UNSIGNED NULL DEFAULT '10',\n" +
                "  C SMALLINT UNSIGNED NULL,\n" +
                "  D SMALLINT UNSIGNED NOT NULL,\n" +
                "  E SMALLINT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F SMALLINT UNSIGNED NOT NULL DEFAULT '0'\n" +
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
    }

    @Test
    public void parseUnsignedMediumintDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_MEDIUMINT_TABLE (\n" +
                "  A MEDIUMINT UNSIGNED NULL DEFAULT 0,\n" +
                "  B MEDIUMINT UNSIGNED NULL DEFAULT '10',\n" +
                "  C MEDIUMINT UNSIGNED NULL,\n" +
                "  D MEDIUMINT UNSIGNED NOT NULL,\n" +
                "  E MEDIUMINT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F MEDIUMINT UNSIGNED NOT NULL DEFAULT '0'\n" +
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
    }

    @Test
    public void parseUnsignedIntDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_INT_TABLE (\n" +
                "  A INT UNSIGNED NULL DEFAULT 0,\n" +
                "  B INT UNSIGNED NULL DEFAULT '10',\n" +
                "  C INT UNSIGNED NULL,\n" +
                "  D INT UNSIGNED NOT NULL,\n" +
                "  E INT UNSIGNED NOT NULL DEFAULT 0,\n" +
                "  F INT UNSIGNED NOT NULL DEFAULT '0'\n" +
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
        converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.PRECISE);
        parser = new MySqlDdlParser(false, converters);
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
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(new BigDecimal(10));
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    public void parseStringDefaultValue() {
        parser = new MySqlDdlParser(false, converters);
        String sql = "CREATE TABLE UNSIGNED_STRING_TABLE (\n" +
                "  A CHAR NULL DEFAULT 'A',\n" +
                "  B CHAR NULL DEFAULT 'b',\n" +
                "  C VARCHAR(10) NULL DEFAULT 'CC',\n" +
                "  D NCHAR(10) NULL DEFAULT '10',\n" +
                "  E NVARCHAR NULL DEFAULT '0',\n" +
                "  F CHAR DEFAULT NULL,\n" +
                "  G VARCHAR(10) DEFAULT NULL,\n" +
                "  H NCHAR(10) DEFAULT NULL\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_STRING_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo("A");
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo("b");
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo("CC");
        assertThat(table.columnWithName("D").defaultValue()).isEqualTo("10");
        assertThat(table.columnWithName("E").defaultValue()).isEqualTo("0");
        assertThat(table.columnWithName("F").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("H").defaultValue()).isEqualTo(null);
    }

    @Test
    public void parseBitDefaultValue() {
        parser = new MySqlDdlParser(false, converters);
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
        assertThat(table.columnWithName("H").defaultValue()).isEqualTo(new byte[] {66, 1});
        assertThat(table.columnWithName("I").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("J").defaultValue()).isEqualTo(new byte[] {15, 97, 1, 0});
    }

    @Test
    public void parseBooleanDefaultValue() {
        parser = new MySqlDdlParser(false, converters);
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
        parser = new MySqlDdlParser(false, converters);
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
        assertThat(table.columnWithName("G").defaultValue()).isEqualTo(1.0d);
    }

    @Test
    public void parseRealDefaultValue() {
        parser = new MySqlDdlParser(false, converters);
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
        parser = new MySqlDdlParser(false, converters);
        String sql = "CREATE TABLE NUMERIC_DECIMAL_TABLE (\n" +
                "  A NUMERIC NOT NULL DEFAULT 1.23,\n" +
                "  B DECIMAL NOT NULL DEFAULT 2.321,\n" +
                "  C NUMERIC NULL DEFAULT '12.678'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMERIC_DECIMAL_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(1.23d);
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(2.321d);
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(12.678d);
    }

    @Test
    public void parseNumericAndDecimalToDecimalDefaultValue() {
        converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG);
        parser = new MySqlDdlParser(false, converters);
        String sql = "CREATE TABLE NUMERIC_DECIMAL_TABLE (\n" +
                "  A NUMERIC NOT NULL DEFAULT 1.23,\n" +
                "  B DECIMAL NOT NULL DEFAULT 2.321,\n" +
                "  C NUMERIC NULL DEFAULT '12.678'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMERIC_DECIMAL_TABLE"));
        assertThat(table.columnWithName("A").defaultValue()).isEqualTo(BigDecimal.valueOf(1.23));
        assertThat(table.columnWithName("B").defaultValue()).isEqualTo(BigDecimal.valueOf(2.321));
        assertThat(table.columnWithName("C").defaultValue()).isEqualTo(BigDecimal.valueOf(12.678));
    }
}
