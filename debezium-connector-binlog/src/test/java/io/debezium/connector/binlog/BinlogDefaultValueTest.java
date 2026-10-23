/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.connector.binlog.jdbc.BinlogSystemVariables.CHARSET_NAME_CLIENT;
import static io.debezium.connector.binlog.jdbc.BinlogSystemVariables.CHARSET_NAME_CONNECTION;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BitDefaultValueTestCases.BitDefaultValueCase;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables.BinlogScope;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.data.EnumeratedValues;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.time.ZonedTimestamp;

/**
 * @author laomei
 */
public abstract class BinlogDefaultValueTest<V extends BinlogValueConverters, P extends DdlParser> {

    protected P parser;
    protected Tables tables;
    private V converters;
    private TableSchemaBuilder tableSchemaBuilder;

    @BeforeEach
    void beforeEach() {
        converters = getValueConverter(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES);
        parser = getDdlParser();
        tables = new Tables();
        tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                getDefaultValueConverter(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.WARN);

    }

    @ParameterizedTest
    @MethodSource("enumAndSetQuotes")
    @FixFor("debezium/dbz#2764")
    void shouldPreserveEnumAndSetQuotes(String literal, String expected, String sqlMode, String type, boolean alter) {
        parser.parse("SET sql_mode = '" + sqlMode + "'", tables);
        if (alter) {
            parser.parse("CREATE TABLE enum_options (v " + type + "('old'))", tables);
        }
        parser.parse((alter ? "ALTER TABLE enum_options MODIFY COLUMN v " : "CREATE TABLE enum_options (v ")
                + type + "(" + literal + ",'other')" + (alter ? "" : ")"), tables);

        final var table = tables.forTable(new TableId(null, null, "enum_options"));
        final var serializer = new JsonTableChangeSerializer();
        final var restored = serializer.deserialize(serializer.serialize(new TableChanges().create(table)), true)
                .iterator().next().getTable();
        parser.parse("SET sql_mode = ''", tables);
        for (final var candidate : List.of(table, restored)) {
            final var column = candidate.columnWithName("v");
            final var schema = getColumnSchema(candidate, "v");
            assertThat(schema.parameters().get("allowed")).isEqualTo(EnumeratedValues.toCommaSeparatedString(List.of(expected, "other")));
            final Object binlogValue = type.equals("ENUM") ? (Object) Integer.valueOf(1) : Long.valueOf(1);
            assertThat(converters.converter(column, new Field("v", 0, schema)).convert(binlogValue)).isEqualTo(expected);
        }
    }

    private static Stream<Arguments> enumAndSetQuotes() {
        return Stream.of(
                Arguments.of("\"a\"\"\"", "a\"", ""),
                Arguments.of("'a''b'", "a'b", ""),
                Arguments.of("\"a''b\"", "a''b", ""),
                Arguments.of("'a\"\"b'", "a\"\"b", ""),
                Arguments.of("'a''b'", "a'b", "ANSI_QUOTES"),
                Arguments.of("\"a\"\"b\"", "a\"b", "NO_BACKSLASH_ESCAPES"))
                .flatMap(arguments -> Stream.of("ENUM", "SET")
                        .flatMap(type -> Stream.of(false, true)
                                .map(alter -> Arguments.of(arguments.get()[0], arguments.get()[1], arguments.get()[2], type, alter))));
    }

    @ParameterizedTest
    @MethodSource("stringDefaults")
    @FixFor("debezium/dbz#2764")
    void shouldDecodeStringDefaults(String literal, String expected, String sqlMode, String statement) {
        parser.parse("SET sql_mode = '" + sqlMode + "'", tables);
        if (statement.startsWith("ALTER")) {
            parser.parse("CREATE TABLE string_defaults (v VARCHAR(64) DEFAULT 'old')", tables);
        }
        parser.parse(statement.formatted(literal), tables);

        final var table = tables.forTable(new TableId(null, null, "string_defaults"));
        assertThat(table.columnWithName("v").defaultValueExpression()).contains(expected);
        assertThat(getColumnSchema(table, "v").defaultValue()).isEqualTo(expected);
    }

    private static Stream<Arguments> stringDefaults() {
        return Stream.of(
                Arguments.of("'a''b'", "a'b", ""),
                Arguments.of("'a\\nb'", "a\nb", ""),
                Arguments.of("N'abc'", "abc", ""),
                Arguments.of("'a' 'b'", "ab", ""),
                Arguments.of("n'a''b' 'c'", "a'bc", ""),
                Arguments.of("_utf8mb4'a''b' 'c'", "a'bc", ""),
                Arguments.of("'a' /* gap */ 'b'", "ab", ""),
                Arguments.of("''", "", ""),
                Arguments.of("'null'", "null", ""),
                Arguments.of("'a\\'b'", "a'b", ""),
                Arguments.of("'a\\'''b'", "a''b", ""),
                Arguments.of("'a\\\"b'", "a\"b", ""),
                Arguments.of("'a\\\\nb'", "a\\nb", ""),
                Arguments.of("'\\0\\b\\n\\r\\t\\Z'", "\0\b\n\r\t\u001a", ""),
                Arguments.of("'\\x\\%\\_'", "x\\%\\_", ""),
                Arguments.of("\"a\"\"b\"", "a\"b", ""),
                Arguments.of("\"a'b\"", "a'b", ""),
                Arguments.of("\"a\\'b\"", "a'b", ""),
                Arguments.of("\"a\\\\'b\"", "a\\'b", ""),
                Arguments.of("\"a\\\"b\"", "a\"b", ""),
                Arguments.of("'a''b'", "a'b", "ANSI_QUOTES"),
                Arguments.of("'a\\nb'", "a\nb", "ANSI_QUOTES"),
                Arguments.of("'a''b'", "a'b", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("'a\\nb'", "a\\nb", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("'a\\'", "a\\", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("N'a\\'", "a\\", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("_utf8mb4'a\\'", "a\\", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("'a\\' 'b'", "a\\b", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("\"a\\\"", "a\\", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("\"a\\'b\"", "a\\'b", "NO_BACKSLASH_ESCAPES"),
                Arguments.of("'a\\'", "a\\", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"))
                .flatMap(arguments -> Stream.of(
                        "CREATE TABLE string_defaults (v VARCHAR(64) DEFAULT %s)",
                        "ALTER TABLE string_defaults MODIFY COLUMN v VARCHAR(64) DEFAULT %s",
                        "ALTER TABLE string_defaults ALTER COLUMN v SET DEFAULT %s")
                        .map(statement -> Arguments.of(arguments.get()[0], arguments.get()[1], arguments.get()[2], statement)));
    }

    @Test
    void parseUnsignedTinyintDefaultValue() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo((short) 0);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo((short) 10);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(getColumnSchema(table, "C").defaultValue()).isNull();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo((short) 0);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo((short) 0);
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo((short) 255);
    }

    @Test
    void parseUnsignedSmallintDefaultValue() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(10);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(getColumnSchema(table, "D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(getColumnSchema(table, "E").isOptional()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(65535);
    }

    @Test
    void parseUnsignedMediumintDefaultValue() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(10);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(16777215);
    }

    @Test
    void parseUnsignedIntDefaultValue() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(10L);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(4294967295L);
    }

    @Test
    void parseUnsignedBigIntDefaultValueToLong() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(10L);
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(getColumnSchema(table, "D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(getColumnSchema(table, "E").isOptional()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(0L);
    }

    @Test
    void parseUnsignedBigIntDefaultValueToBigDecimal() {
        final V converters = getValueConverter(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                BinaryHandlingMode.BYTES);
        final P parser = getDdlParser();
        final TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                getDefaultValueConverter(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.WARN);

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
        assertThat(getColumnSchema(table, "A", tableSchemaBuilder).defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(getColumnSchema(table, "B", tableSchemaBuilder).defaultValue()).isEqualTo(new BigDecimal(10));
        assertThat(table.columnWithName("C").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("C").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("D").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("D").hasDefaultValue()).isFalse();
        assertThat(table.columnWithName("E").isOptional()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E", tableSchemaBuilder).defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(getColumnSchema(table, "F", tableSchemaBuilder).defaultValue()).isEqualTo(BigDecimal.ZERO);
        assertThat(getColumnSchema(table, "G", tableSchemaBuilder).defaultValue()).isEqualTo(new BigDecimal("18446744073709551615"));
    }

    @Test
    public void parseStringDefaultValue() {
        String sql = "CREATE TABLE UNSIGNED_STRING_TABLE (\n" +
                "  A CHAR NULL DEFAULT 'A',\n" +
                "  B CHAR CHARACTER SET utf8 NULL DEFAULT 'b',\n" +
                "  C VARCHAR(10) NULL DEFAULT 'CC',\n" +
                "  D NCHAR(10) NULL DEFAULT '10',\n" +
                "  E NVARCHAR(100) NULL DEFAULT '0',\n" +
                "  F CHAR DEFAULT NULL,\n" +
                "  G VARCHAR(10) DEFAULT NULL,\n" +
                "  H NCHAR(10) DEFAULT NULL\n" +
                ") CHARACTER SET 'latin2';";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "UNSIGNED_STRING_TABLE"));
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo("A");
        assertThat(table.columnWithName("A").charsetName()).isEqualTo("latin2");
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo("b");
        assertThat(table.columnWithName("B").charsetName()).isEqualTo("utf8");
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo("CC");
        assertThat(getColumnSchema(table, "D").defaultValue()).isEqualTo("10");
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo("0");
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "H").defaultValue()).isEqualTo(null);
    }

    @Test
    void parseBitDefaultValue() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(false);
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "D").defaultValue()).isEqualTo(false);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(false);
        assertThat(getColumnSchema(table, "H").defaultValue()).isEqualTo(new byte[]{ 66, 1 });
        assertThat(getColumnSchema(table, "I").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "J").defaultValue()).isEqualTo(new byte[]{ 15, 97, 1, 0 });
    }

    @Test
    void parseBooleanDefaultValue() {
        String sql = "CREATE TABLE BOOLEAN_TABLE (\n" +
                "  A BOOLEAN NULL DEFAULT 0,\n" +
                "  B BOOLEAN NOT NULL DEFAULT '1',\n" +
                "  C BOOLEAN NOT NULL DEFAULT '9',\n" +
                "  D BOOLEAN NOT NULL DEFAULT TRUE,\n" +
                "  E BOOLEAN DEFAULT NULL\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "BOOLEAN_TABLE"));
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(false);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "D").defaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(null);
    }

    @Test
    void parseNumberDefaultValue() {
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

        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo((byte) 10);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo((short) 5);
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "D").defaultValue()).isEqualTo(20L);
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(0f);
    }

    @Test
    void parseRealDefaultValue() {
        String sql = "CREATE TABLE REAL_TABLE (\n" +
                "  A REAL NOT NULL DEFAULT 1,\n" +
                "  B REAL NULL DEFAULT NULL \n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "REAL_TABLE"));
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(1d);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(null);
    }

    @Test
    void parseNumericAndDecimalToDoubleDefaultValue() {
        String sql = "CREATE TABLE NUMERIC_DECIMAL_TABLE (\n" +
                "  A NUMERIC NOT NULL DEFAULT 1.23,\n" +
                "  B DECIMAL(5,3) NOT NULL DEFAULT 2.321,\n" +
                "  C NUMERIC NULL DEFAULT '12.678',\n" +
                "  D DECIMAL(5,2) NULL DEFAULT '12.678'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMERIC_DECIMAL_TABLE"));
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo(1.0d);
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo(2.321d);
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo(13d);
        assertThat(getColumnSchema(table, "D").defaultValue()).isEqualTo(12.68d);
    }

    @Test
    void parseNumericAndDecimalToDecimalDefaultValue() {
        final V converters = getValueConverter(JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES);
        final P parser = getDdlParser();
        final TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                getDefaultValueConverter(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.WARN);
        String sql = "CREATE TABLE NUMERIC_DECIMAL_TABLE (\n" +
                "  A NUMERIC NOT NULL DEFAULT 1.23,\n" +
                "  B DECIMAL(5,3) NOT NULL DEFAULT 2.321,\n" +
                "  C NUMERIC NULL DEFAULT '12.678'\n" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "NUMERIC_DECIMAL_TABLE"));
        assertThat(getColumnSchema(table, "A", tableSchemaBuilder).defaultValue()).isEqualTo(BigDecimal.valueOf(1));
        assertThat(getColumnSchema(table, "B", tableSchemaBuilder).defaultValue()).isEqualTo(BigDecimal.valueOf(2.321));
        assertThat(getColumnSchema(table, "C", tableSchemaBuilder).defaultValue()).isEqualTo(BigDecimal.valueOf(13));
    }

    @Test
    void parseTimeDefaultValue() {
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(getColumnSchema(table, "D").defaultValue())
                .isEqualTo(ZonedTimestamp.toIsoString(LocalDateTime.of(2018, 6, 26, 12, 34, 56, 0).atZone(ZoneId.systemDefault()), null, null));
        assertThat(getColumnSchema(table, "E").defaultValue())
                .isEqualTo(ZonedTimestamp.toIsoString(LocalDateTime.of(2018, 6, 26, 12, 34, 56, 0).atZone(ZoneId.systemDefault()), null, null));
        assertThat(getColumnSchema(table, "F").defaultValue())
                .isEqualTo(ZonedTimestamp.toIsoString(LocalDateTime.of(2018, 6, 26, 12, 34, 56, 780_000_000).atZone(ZoneId.systemDefault()), null, null));
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(Date.from(Instant.ofEpochMilli(0)));
        assertThat(getColumnSchema(table, "H").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "I").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "J").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 6, 26, 12, 34, 56, 0, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "K").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 6, 26, 12, 34, 56, 0, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "L").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 6, 26, 12, 34, 56, 780_000_000, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "M").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "N").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "O").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
    }

    @Test
    void parseDateDefaultValue() {
        String sql = "CREATE TABLE DATE_TABLE (" +
                "  A date NOT NULL DEFAULT '0000-00-00'," +
                "  B date NOT NULL DEFAULT '2018-00-01'," +
                "  C date NOT NULL DEFAULT '0000-12-31'," +
                "  D date NOT NULL DEFAULT '2018-01-00'," +
                "  E date NOT NULL DEFAULT '9999-09-09'," +
                "  F date NOT NULL DEFAULT '1111-11-11'," +
                "  G date NOT NULL DEFAULT '2018-08-31'," +
                "  H date NOT NULL DEFAULT '2050-01-01 00:00:00'," +
                "  I date NOT NULL DEFAULT 0" +
                ");";
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "DATE_TABLE"));
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "C").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "D").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "E").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(9999, 9, 9, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "F").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(1111, 11, 11, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "G").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2018, 8, 31, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "H").defaultValue()).isEqualTo(Date.from(ZonedDateTime.of(2050, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant()));
        assertThat(getColumnSchema(table, "I").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
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
        assertThat(getColumnSchema(table, "A").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
        assertThat(getColumnSchema(table, "B").defaultValue()).isEqualTo((Date.from(Instant.ofEpochMilli(0))));
    }

    @Test
    @FixFor("DBZ-870")
    public void shouldAcceptZeroAsDefaultValueForDateColumn() {
        String ddl = "CREATE TABLE data(id INT, nullable_date date default 0, not_nullable_date date not null default 0, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat(table.columnWithName("nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to null for nullable column
        assertThat(getColumnSchema(table, "nullable_date").defaultValue()).isNull();

        assertThat(table.columnWithName("not_nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to epoch for non-nullable column (expecting Date, as this test is using "connect"
        // mode)
        assertThat(getColumnSchema(table, "not_nullable_date").defaultValue()).isEqualTo(getEpochDate());
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

        assertThat((Boolean) getColumnSchema(table, "bval").defaultValue()).isTrue();
        assertThat((Byte) getColumnSchema(table, "tival1").defaultValue()).isZero();
        assertThat((Byte) getColumnSchema(table, "tival2").defaultValue()).isEqualTo((byte) 3);
        assertThat((Byte) getColumnSchema(table, "tival3").defaultValue()).isEqualTo((byte) 1);
        assertThat((Byte) getColumnSchema(table, "tival4").defaultValue()).isEqualTo((byte) 18);
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

        assertThat((Boolean) getColumnSchema(table, "bval").defaultValue()).isTrue();
        assertThat((Integer) getColumnSchema(table, "ival1").defaultValue()).isZero();
        assertThat((Integer) getColumnSchema(table, "ival2").defaultValue()).isEqualTo(3);
        assertThat((Integer) getColumnSchema(table, "ival3").defaultValue()).isEqualTo(1);
        assertThat((Integer) getColumnSchema(table, "ival4").defaultValue()).isEqualTo(18);
    }

    @Test
    @FixFor("DBZ-1249")
    public void shouldAcceptBitSetDefaultValue() {
        String ddl = "CREATE TABLE user_subscribe (id bigint(20) unsigned NOT NULL AUTO_INCREMENT, content bit(24) DEFAULT b'111111111111101100001110', PRIMARY KEY (id)) ENGINE=InnoDB";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "user_subscribe"));

        final byte[] defVal = (byte[]) getColumnSchema(table, "content").defaultValue();
        assertThat(Byte.toUnsignedInt((defVal[0]))).isEqualTo(0b00001110);
        assertThat(Byte.toUnsignedInt((defVal[1]))).isEqualTo(0b11111011);
        assertThat(Byte.toUnsignedInt((defVal[2]))).isEqualTo(0b11111111);
    }

    @Test
    @FixFor("DBZ-3541")
    public void shouldRoundIntExpressedAsDecimal() {
        String ddl = "CREATE TABLE int_as_decimal (col1 INT DEFAULT '0.0', col2 INT DEFAULT '1.5')";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "int_as_decimal"));

        assertThat(getColumnSchema(table, "col1").defaultValue()).isEqualTo(0);
        assertThat(getColumnSchema(table, "col2").defaultValue()).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-3541")
    public void shouldParseScientificNotation() {
        String ddl = "CREATE TABLE int_as_e (col1 INT DEFAULT 1E1, col2 INT DEFAULT 15E-1)";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "int_as_e"));

        assertThat(getColumnSchema(table, "col1").defaultValue()).isEqualTo(10);
        assertThat(getColumnSchema(table, "col2").defaultValue()).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-3541")
    public void shouldParseStringScientificNotation() {
        String ddl = "CREATE TABLE int_as_e (col1 INT DEFAULT 1E1, col2 INT DEFAULT '15E-1')";

        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "int_as_e"));

        assertThat(getColumnSchema(table, "col1").defaultValue()).isEqualTo(10);
        assertThat(getColumnSchema(table, "col2").defaultValue()).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-3989")
    public void shouldTrimNumericalDefaultValueAndShouldNotTrimNonNumericalDefaultValue() {
        String ddl = "CREATE TABLE data(id INT DEFAULT '1 ', data VARCHAR(3) DEFAULT ' 3 ')";
        parser.parse(ddl, tables);
        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat((Integer) getColumnSchema(table, "id").defaultValue()).isEqualTo(1);
        assertThat((String) getColumnSchema(table, "data").defaultValue()).isEqualTo(" 3 ");
    }

    @Test
    @FixFor("DBZ-5134")
    public void parseNumericAndDecimalToIntDefaultValue() {
        final P parser = getDdlParser();
        final TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                getDefaultValueConverter(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.WARN);
        String ddl = "CREATE TABLE `tbl_default` (  \n"
                + "`id` int(11) NOT NULL AUTO_INCREMENT,\n"
                + "c0 tinyint not null default '10.01',\n"
                + "c1 int not null default '5.234',\n"
                + "c2 bigint not null default '0.000000000000000000',\n"
                + "c3 bigint not null default .12345,\n"
                + "c4 smallint not null default 100.52345,\n"
                + "c5 int not null default '-.789',\n"
                + "c6 decimal(26,6) default \"1\",\n"
                + "PRIMARY KEY (`id`)\n"
                + ")";
        parser.parse(ddl, tables);
        Table table = tables.forTable(new TableId(null, null, "tbl_default"));
        assertThat(((AntlrDdlParser<?, ?>) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        TableSchema schema = tableSchemaBuilder.create(defaultTopicNamingStrategy(), table, null, null, null);
        assertThat(getColumnSchema(schema, "c0").defaultValue()).isEqualTo((byte) 10);
        assertThat(getColumnSchema(schema, "c1").defaultValue()).isEqualTo(5);
        assertThat(getColumnSchema(schema, "c2").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(schema, "c3").defaultValue()).isEqualTo(0L);
        assertThat(getColumnSchema(schema, "c4").defaultValue()).isEqualTo(Short.valueOf("101"));
        assertThat(getColumnSchema(schema, "c5").defaultValue()).isEqualTo(-1);
        assertThat(getColumnSchema(schema, "c6").defaultValue()).isEqualTo(1.0);
    }

    @ParameterizedTest
    @MethodSource("integerDefaults")
    @FixFor("debezium/dbz#2763")
    void shouldPreserveIntegerDefaults(String type, String literal, long expected) {
        final var tableId = new TableId(null, null, "integer_defaults");
        for (String expression : List.of(literal, "'" + literal + "'")) {
            final var ddl = "CREATE TABLE integer_defaults (v " + type + " NOT NULL DEFAULT " + expression + ")";
            parser.parse(ddl, tables);
            final var value = getColumnSchema(tables.forTable(tableId), "v").defaultValue();
            assertThat(value).as(ddl).isInstanceOf(Number.class);
            assertThat(((Number) value).longValue()).as(ddl).isEqualTo(expected);
        }
    }

    protected static Stream<Arguments> integerDefaults() {
        final var rounding = Stream.of("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT")
                .flatMap(type -> Stream.of(
                        Arguments.of(type, "-1.6", -2L),
                        Arguments.of(type, "-1.5", -2L),
                        Arguments.of(type, "-1.4", -1L),
                        Arguments.of(type, "-0.5", -1L),
                        Arguments.of(type, "0.5", 1L),
                        Arguments.of(type, "1.4", 1L),
                        Arguments.of(type, "1.5", 2L),
                        Arguments.of(type, "1.6", 2L),
                        Arguments.of(type, "1.4999999999999999", 1L),
                        Arguments.of(type, "-15e-1", -2L)));
        final var precision = Stream.of(
                Arguments.of("BIGINT", "9007199254740993", 9007199254740993L),
                Arguments.of("BIGINT", "-9007199254740993", -9007199254740993L),
                Arguments.of("BIGINT", "9223372036854775806", Long.MAX_VALUE - 1),
                Arguments.of("BIGINT", "9223372036854775807", Long.MAX_VALUE),
                Arguments.of("BIGINT", "-9223372036854775807", Long.MIN_VALUE + 1),
                Arguments.of("BIGINT", "-9223372036854775808", Long.MIN_VALUE),
                Arguments.of("BIGINT", "9007199254740992.5", 9007199254740993L),
                Arguments.of("BIGINT", "-9007199254740992.5", -9007199254740993L));
        return Stream.concat(rounding, precision);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.debezium.connector.binlog.BitDefaultValueTestCases#cases")
    @FixFor("debezium/dbz#2751")
    void shouldPreserveBitDefaultValues(BitDefaultValueCase testCase) {
        assertBitDefaultValue(testCase);
    }

    protected void assertBitDefaultValue(BitDefaultValueCase testCase) {
        final var tableId = new TableId(null, null, "bit_defaults");
        final String columnType = "bits BIT(" + testCase.width() + ")" + (testCase.nullable() ? " NULL" : " NOT NULL");
        final String columnDefinition = columnType + " DEFAULT " + testCase.literal();
        final String ddl;

        switch (testCase.action()) {
            case "create-table":
                ddl = "CREATE TABLE bit_defaults (id INT PRIMARY KEY, " + columnDefinition + ")";
                break;
            case "add-column":
                parser.parse("CREATE TABLE bit_defaults (id INT PRIMARY KEY)", tables);
                ddl = "ALTER TABLE bit_defaults ADD COLUMN " + columnDefinition;
                break;
            case "modify-column":
                parser.parse("CREATE TABLE bit_defaults (id INT PRIMARY KEY, " + columnType + " DEFAULT " + testCase.initialDefaultLiteral() + ")", tables);
                ddl = "ALTER TABLE bit_defaults MODIFY COLUMN " + columnDefinition;
                break;
            case "alter-default":
                parser.parse("CREATE TABLE bit_defaults (id INT PRIMARY KEY, " + columnType + " DEFAULT " + testCase.initialDefaultLiteral() + ")", tables);
                assertThat(getColumnSchema(tables.forTable(tableId), "bits").defaultValue()).isNotNull();
                ddl = "ALTER TABLE bit_defaults ALTER COLUMN bits SET DEFAULT " + testCase.literal();
                break;
            default:
                throw new IllegalArgumentException("Unexpected DDL action: " + testCase.action());
        }

        parser.parse(ddl, tables);

        final var schema = getColumnSchema(tables.forTable(tableId), "bits");
        assertThat(schema.isOptional()).isEqualTo(testCase.nullable());
        assertThat(schema.type()).isEqualTo(testCase.width() == 1 ? Schema.Type.BOOLEAN : Schema.Type.BYTES);
        assertThat(schema.defaultValue()).isEqualTo(expectedBitDefault(testCase));
    }

    @Test
    @FixFor("debezium/dbz#2751")
    void shouldUseSessionCharacterSetsForBitStringDefaults() {
        parser.systemVariables().setVariable(BinlogScope.SESSION, CHARSET_NAME_CLIENT, "utf8mb4");
        parser.systemVariables().setVariable(BinlogScope.SESSION, CHARSET_NAME_CONNECTION, "latin1");
        parser.parse("CREATE TABLE bit_string_charsets ("
                + "plain_value BIT(64) DEFAULT 'é', "
                + "adjacent_value BIT(64) DEFAULT 'é' 'é', "
                + "national_value BIT(64) DEFAULT N'é' 'é', "
                + "introduced_value BIT(64) DEFAULT _latin1'é' 'é')", tables);

        final var table = tables.forTable(new TableId(null, null, "bit_string_charsets"));
        assertThat(getColumnSchema(table, "plain_value").defaultValue())
                .isEqualTo(new byte[]{ (byte) 0xE9, 0, 0, 0, 0, 0, 0, 0 });
        assertThat(getColumnSchema(table, "adjacent_value").defaultValue())
                .isEqualTo(new byte[]{ (byte) 0xE9, (byte) 0xE9, 0, 0, 0, 0, 0, 0 });
        assertThat(getColumnSchema(table, "national_value").defaultValue())
                .isEqualTo(new byte[]{ (byte) 0xE9, (byte) 0xA9, (byte) 0xC3, 0, 0, 0, 0, 0 });
        assertThat(getColumnSchema(table, "introduced_value").defaultValue())
                .isEqualTo(new byte[]{ (byte) 0xE9, (byte) 0xA9, (byte) 0xC3, 0, 0, 0, 0, 0 });
    }

    @ParameterizedTest(name = "{0} DEFAULT {1}, {3}")
    @MethodSource("binaryLiteralDefaults")
    @FixFor("debezium/dbz#2765")
    void shouldConvertHexadecimalAndBitValueDefaults(String type, String literal, String equivalentLiteral, String action) {
        assertBinaryLiteralDefault(type, literal, equivalentLiteral, action);
    }

    protected static Stream<Arguments> binaryLiteralDefaults() {
        // Each literal is paired with the literal that SHOW CREATE TABLE, and thus a snapshot, uses for the stored default
        return withDdlActions(Stream.of(
                Arguments.of("INT", "0x10", "'16'"),
                Arguments.of("INT", "b'10'", "'2'"),
                Arguments.of("INT", "0b10", "'2'"),
                Arguments.of("TINYINT UNSIGNED", "0xFF", "'255'"),
                Arguments.of("BIGINT UNSIGNED", "0xFFFFFFFFFFFFFFFF", "'18446744073709551615'"),
                Arguments.of("DECIMAL(10,2)", "0x10", "'16.00'"),
                Arguments.of("DOUBLE", "b'10'", "'2'"),
                Arguments.of("YEAR", "0x07E8", "'2024'"),
                Arguments.of("BOOLEAN", "0x01", "'1'"),
                Arguments.of("VARCHAR(32)", "0x4142", "'AB'"),
                Arguments.of("VARCHAR(32)", "X'4142'", "'AB'"),
                Arguments.of("VARCHAR(32)", "b'01000001'", "'A'"),
                Arguments.of("VARCHAR(32)", "0b1000001", "'A'"),
                Arguments.of("VARCHAR(32) CHARACTER SET utf8mb4", "0xE4BDA0", "'你'"),
                Arguments.of("VARCHAR(32) CHARACTER SET latin1", "0xE9", "'é'"),
                Arguments.of("VARCHAR(32) CHARACTER SET latin1", "_utf8mb4 0xC3A9", "'é'"),
                Arguments.of("CHAR(4)", "0x41", "'A'"),
                Arguments.of("ENUM('a','b')", "0x62", "'b'"),
                Arguments.of("SET('a','b')", "0x612C62", "'a,b'"),
                Arguments.of("DATE", "0x323032342D30312D3032", "'2024-01-02'"),
                Arguments.of("BINARY(2)", "0x4142", "'AB'"),
                Arguments.of("VARBINARY(16)", "0x4142", "'AB'"),
                // A character set introducer makes the literal a string, also in a numeric context
                Arguments.of("INT", "_latin1 0x31", "'1'")));
    }

    protected static Stream<Arguments> withDdlActions(Stream<Arguments> cases) {
        final List<Arguments> list = cases.toList();
        return Stream.of("create-table", "add-column", "modify-column", "alter-default")
                .flatMap(action -> list.stream().map(arguments -> Arguments.of(arguments.get()[0], arguments.get()[1], arguments.get()[2], action)));
    }

    protected void assertBinaryLiteralDefault(String type, String literal, String equivalentLiteral, String action) {
        final Object expected = parseDefaultValue(type, equivalentLiteral, "create-table");
        assertThat(expected).isNotNull();
        assertThat(parseDefaultValue(type, literal, action)).isEqualTo(expected);
    }

    @Test
    @FixFor("debezium/dbz#2765")
    void shouldKeepLeadingZeroBytesOfHexadecimalAndBitValueDefaults() {
        assertThat(parseDefaultValue("VARCHAR(32)", "0x041", "create-table")).isEqualTo("\0A");
        assertThat(parseDefaultValue("VARCHAR(32)", "b'0000000001000001'", "alter-default")).isEqualTo("\0A");
    }

    @Test
    @FixFor("debezium/dbz#2765")
    void shouldDecodeHexadecimalDefaultsInDatabaseCharacterSet() {
        parser.parse("CREATE DATABASE latin1_db CHARACTER SET latin1; USE latin1_db;", tables);
        parser.parse("CREATE TABLE latin1_defaults (v VARCHAR(32) DEFAULT 0xE9)", tables);
        assertThat(getColumnSchema(tables.forTable(new TableId("latin1_db", null, "latin1_defaults")), "v").defaultValue()).isEqualTo("é");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("binaryLiteralDefaultsInCharacterSetOfColumn")
    @FixFor("debezium/dbz#2765")
    void shouldDecodeHexadecimalDefaultsInCharacterSetOfColumn(String description, String databaseCharset, String ddl) {
        // MySQL and MariaDB store each of these defaults as 'é'
        assertThat(parseDefaultValueOfColumnV(databaseCharset, ddl)).isEqualTo("é");
    }

    protected static Stream<Arguments> binaryLiteralDefaultsInCharacterSetOfColumn() {
        // A column without a character set uses the one of its table as it is at the end of the statement
        return Stream.of(
                Arguments.of("CREATE TABLE of a latin1 table", "utf8mb4",
                        "CREATE TABLE t (id INT, v VARCHAR(32) DEFAULT 0xE9) CHARACTER SET latin1"),
                Arguments.of("CREATE TABLE of a utf8mb4 table", "latin1",
                        "CREATE TABLE t (id INT, v VARCHAR(32) DEFAULT 0xC3A9) CHARACTER SET utf8mb4"),
                Arguments.of("CREATE TABLE with a table collation", "utf8mb4",
                        "CREATE TABLE t (id INT, v VARCHAR(32) DEFAULT 0xE9) COLLATE latin1_bin"),
                Arguments.of("CREATE TABLE with a column character set", "latin1",
                        "CREATE TABLE t (id INT, v VARCHAR(32) CHARACTER SET utf8mb4 DEFAULT 0xC3A9) CHARACTER SET latin1"),
                Arguments.of("CREATE TABLE in another database", "utf8mb4",
                        "CREATE DATABASE other_db CHARACTER SET latin1; CREATE TABLE other_db.t (id INT, v VARCHAR(32) DEFAULT 0xE9)"),
                Arguments.of("ADD COLUMN to a latin1 table", "utf8mb4",
                        "CREATE TABLE t (id INT) CHARACTER SET latin1; ALTER TABLE t ADD COLUMN v VARCHAR(32) DEFAULT 0xE9"),
                Arguments.of("ADD COLUMN to a utf8mb4 table", "latin1",
                        "CREATE TABLE t (id INT) CHARACTER SET utf8mb4; ALTER TABLE t ADD COLUMN v VARCHAR(32) DEFAULT 0xC3A9"),
                Arguments.of("ADD COLUMN with several columns", "utf8mb4",
                        "CREATE TABLE t (id INT) CHARACTER SET latin1; ALTER TABLE t ADD COLUMN (v VARCHAR(32) DEFAULT 0xE9, w INT)"),
                Arguments.of("ADD COLUMN after a table character set", "latin1",
                        "CREATE TABLE t (id INT) CHARACTER SET latin1; ALTER TABLE t CHARACTER SET utf8mb4, ADD COLUMN v VARCHAR(32) DEFAULT 0xC3A9"),
                Arguments.of("ADD COLUMN before a table character set", "latin1",
                        "CREATE TABLE t (id INT) CHARACTER SET latin1; ALTER TABLE t ADD COLUMN v VARCHAR(32) DEFAULT 0xC3A9, CHARACTER SET utf8mb4"),
                Arguments.of("MODIFY COLUMN", "utf8mb4",
                        "CREATE TABLE t (id INT, v VARCHAR(32)) CHARACTER SET latin1; ALTER TABLE t MODIFY COLUMN v VARCHAR(32) DEFAULT 0xE9"),
                Arguments.of("MODIFY COLUMN without its former character set", "utf8mb4",
                        "CREATE TABLE t (id INT, v VARCHAR(32) CHARACTER SET utf8mb4) CHARACTER SET latin1; "
                                + "ALTER TABLE t MODIFY COLUMN v VARCHAR(32) DEFAULT 0xE9"),
                Arguments.of("CHANGE COLUMN with a new name", "utf8mb4",
                        "CREATE TABLE t (id INT, u VARCHAR(32)) CHARACTER SET latin1; ALTER TABLE t CHANGE COLUMN u v VARCHAR(32) DEFAULT 0xE9"),
                Arguments.of("SET DEFAULT", "utf8mb4",
                        "CREATE TABLE t (id INT, v VARCHAR(32)) CHARACTER SET latin1; ALTER TABLE t ALTER COLUMN v SET DEFAULT 0xE9"),
                Arguments.of("SET DEFAULT of a column with a character set", "latin1",
                        "CREATE TABLE t (id INT, v VARCHAR(32) CHARACTER SET utf8mb4) CHARACTER SET latin1; ALTER TABLE t ALTER COLUMN v SET DEFAULT 0xC3A9"));
    }

    @Test
    @FixFor("debezium/dbz#2765")
    void shouldUseLastDefaultOfColumnDefinition() {
        assertThat(parseDefaultValueOfColumnV("utf8mb4", "CREATE TABLE t (id INT, v VARCHAR(32) DEFAULT 0xE9 DEFAULT 'y') CHARACTER SET latin1"))
                .isEqualTo("y");
    }

    protected Object parseDefaultValueOfColumnV(String databaseCharset, String ddl) {
        final var charsetTables = new Tables();
        parser.parse("CREATE DATABASE charset_db CHARACTER SET " + databaseCharset + "; USE charset_db;", charsetTables);
        parser.parse(ddl, charsetTables);
        final TableId tableId = charsetTables.tableIds().stream().filter(id -> id.table().equals("t")).findFirst().orElseThrow();
        return getColumnSchema(charsetTables.forTable(tableId), "v").defaultValue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "create-table", "add-column", "modify-column", "alter-default" })
    @FixFor("debezium/dbz#2765")
    void shouldOmitBinaryDefaultThatIsNotValidUtf8(String action) {
        // The default value of a binary column is converted from a UTF-8 string, which cannot represent these bytes
        assertThat(parseDefaultValue("VARBINARY(16)", "0x00FF41", action)).isNull();
    }

    private Object parseDefaultValue(String type, String literal, String action) {
        final var literalTables = new Tables();
        final String column = "v " + type + " DEFAULT " + literal;
        switch (action) {
            case "create-table" -> parser.parse("CREATE TABLE literal_defaults (id INT, " + column + ")", literalTables);
            case "add-column" -> {
                parser.parse("CREATE TABLE literal_defaults (id INT)", literalTables);
                parser.parse("ALTER TABLE literal_defaults ADD COLUMN " + column, literalTables);
            }
            case "modify-column" -> {
                // Start from another default, so that a literal that is not converted cannot keep a matching default
                parser.parse("CREATE TABLE literal_defaults (id INT, v " + type + " DEFAULT NULL)", literalTables);
                parser.parse("ALTER TABLE literal_defaults MODIFY COLUMN " + column, literalTables);
            }
            case "alter-default" -> {
                parser.parse("CREATE TABLE literal_defaults (id INT, v " + type + " DEFAULT NULL)", literalTables);
                parser.parse("ALTER TABLE literal_defaults ALTER COLUMN v SET DEFAULT " + literal, literalTables);
            }
            default -> throw new IllegalArgumentException("Unexpected DDL action: " + action);
        }
        return getColumnSchema(literalTables.forTable(new TableId(null, null, "literal_defaults")), "v").defaultValue();
    }

    @Test
    @FixFor("debezium/dbz#2766")
    void shouldPadBinaryDefaultsWithZeroBytes() {
        // SHOW CREATE TABLE returns the default with the padding that MySQL and MariaDB store
        parser.parse("CREATE TABLE binary_defaults (id INT, "
                + "bin_escaped BINARY(8) NOT NULL DEFAULT 'abc\\0\\0\\0\\0\\0', "
                + "bin_literal BINARY(8) NOT NULL DEFAULT 'abc', "
                + "bin_blank BINARY(4) NOT NULL DEFAULT 'a', "
                + "bin_spaced BINARY(6) NOT NULL DEFAULT 'a ', "
                + "varbin VARBINARY(8) NOT NULL DEFAULT 'abc')", tables);
        parser.parse("ALTER TABLE binary_defaults ALTER COLUMN bin_blank SET DEFAULT ''", tables);
        parser.parse("ALTER TABLE binary_defaults MODIFY COLUMN bin_literal BINARY(8) NOT NULL DEFAULT 'abc'", tables);
        parser.parse("ALTER TABLE binary_defaults ADD COLUMN bin_added BINARY(4) NOT NULL DEFAULT 'ab'", tables);

        final Table table = tables.forTable(new TableId(null, null, "binary_defaults"));
        assertThat(hex(getColumnSchema(table, "bin_escaped").defaultValue())).isEqualTo("6162630000000000");
        assertThat(hex(getColumnSchema(table, "bin_literal").defaultValue())).isEqualTo("6162630000000000");
        assertThat(hex(getColumnSchema(table, "bin_blank").defaultValue())).isEqualTo("00000000");
        assertThat(hex(getColumnSchema(table, "bin_spaced").defaultValue())).isEqualTo("612000000000");
        assertThat(hex(getColumnSchema(table, "bin_added").defaultValue())).isEqualTo("61620000");
        assertThat(hex(getColumnSchema(table, "varbin").defaultValue())).isEqualTo("616263");
    }

    @Test
    @FixFor("debezium/dbz#2766")
    void shouldPadBinaryDefaultsInHexBinaryHandlingMode() {
        final V converters = getValueConverter(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.HEX);
        final TableSchemaBuilder tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                getDefaultValueConverter(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.WARN);
        parser.parse("CREATE TABLE binary_defaults (id INT, bin_literal BINARY(8) NOT NULL DEFAULT 'abc')", tables);

        final Table table = tables.forTable(new TableId(null, null, "binary_defaults"));
        assertThat(getColumnSchema(table, "bin_literal", tableSchemaBuilder).defaultValue()).isEqualTo("6162630000000000");
    }

    private static String hex(Object bytes) {
        final ByteBuffer buffer = ((ByteBuffer) bytes).duplicate();
        final byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return HexFormat.of().formatHex(array);
    }

    private static Object expectedBitDefault(BitDefaultValueCase testCase) {
        if ("NULL".equals(testCase.expectedValue())) {
            return null;
        }
        if (testCase.width() == 1) {
            return !"0".equals(testCase.expectedValue());
        }
        final var value = new BigInteger(testCase.expectedValue());
        final byte[] bytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value.longValue()).array();
        return Arrays.copyOf(bytes, (testCase.width() + Byte.SIZE - 1) / Byte.SIZE);
    }

    @ParameterizedTest(name = "{0} DEFAULT {1}, {3}")
    @MethodSource("compactTemporalDefaults")
    @FixFor("debezium/dbz#2767")
    void shouldConvertTemporalDefaultsWithoutDelimiters(String type, String literal, String normalizedLiteral, String action) {
        // The default temporal precision mode represents negative TIME values and TIME values of more than 24 hours
        final V adaptiveConverters = getValueConverter(JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES);
        final TableSchemaBuilder adaptiveTableSchemaBuilder = new TableSchemaBuilder(
                adaptiveConverters,
                getDefaultValueConverter(adaptiveConverters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false,
                EventConvertingFailureHandlingMode.WARN);

        // A snapshot reads the default in the normalized form that SHOW CREATE TABLE returns
        final Object expected = getColumnSchema(parseTable(type, normalizedLiteral, "create-table"), "v", adaptiveTableSchemaBuilder).defaultValue();
        assertThat(expected).isNotNull();
        assertThat(getColumnSchema(parseTable(type, literal, action), "v", adaptiveTableSchemaBuilder).defaultValue()).isEqualTo(expected);
    }

    protected static Stream<Arguments> compactTemporalDefaults() {
        return withDdlActions(Stream.of(
                Arguments.of("DATETIME", "20260928123456", "'2026-09-28 12:34:56'"),
                Arguments.of("DATETIME", "'20260928123456'", "'2026-09-28 12:34:56'"),
                Arguments.of("DATETIME", "260928123456", "'2026-09-28 12:34:56'"),
                Arguments.of("DATETIME", "'260928123456'", "'2026-09-28 12:34:56'"),
                Arguments.of("DATETIME", "991231235959", "'1999-12-31 23:59:59'"),
                Arguments.of("DATETIME", "20260928", "'2026-09-28 00:00:00'"),
                Arguments.of("DATETIME", "'20260928'", "'2026-09-28 00:00:00'"),
                Arguments.of("DATETIME", "260928", "'2026-09-28 00:00:00'"),
                Arguments.of("DATETIME(3)", "20260928123456.789", "'2026-09-28 12:34:56.789'"),
                Arguments.of("DATETIME(3)", "'20260928123456.789'", "'2026-09-28 12:34:56.789'"),
                Arguments.of("TIMESTAMP", "20260928123456", "'2026-09-28 12:34:56'"),
                Arguments.of("TIMESTAMP", "'20260928123456'", "'2026-09-28 12:34:56'"),
                Arguments.of("TIMESTAMP", "20260928", "'2026-09-28 00:00:00'"),
                Arguments.of("TIMESTAMP(3)", "20260928123456.789", "'2026-09-28 12:34:56.789'"),
                Arguments.of("TIME", "123456", "'12:34:56'"),
                Arguments.of("TIME", "'123456'", "'12:34:56'"),
                Arguments.of("TIME", "3456", "'00:34:56'"),
                Arguments.of("TIME", "'3456'", "'00:34:56'"),
                Arguments.of("TIME", "56", "'00:00:56'"),
                Arguments.of("TIME", "-123456", "'-12:34:56'"),
                Arguments.of("TIME", "'-123456'", "'-12:34:56'"),
                Arguments.of("TIME", "8385959", "'838:59:59'"),
                Arguments.of("TIME(3)", "123456.789", "'12:34:56.789'")));
    }

    @ParameterizedTest(name = "{0} DEFAULT {1}, {3}")
    @MethodSource("enumAndSetDefaults")
    @FixFor("debezium/dbz#2768")
    void shouldConvertEnumAndSetDefaultsToDeclaredMembers(String type, String literal, String expected, String action) {
        // MySQL and MariaDB store the members in the spelling and the order in which they are declared
        assertThat(getColumnSchema(parseTable(type, literal, action), "v").defaultValue()).isEqualTo(expected);
    }

    protected static Stream<Arguments> enumAndSetDefaults() {
        return withDdlActions(Stream.of(
                Arguments.of("SET('a','b','c')", "'b,a'", "a,b"),
                Arguments.of("SET('a','b','c')", "'c,a,b'", "a,b,c"),
                Arguments.of("SET('a','b','c')", "'B,a'", "a,b"),
                Arguments.of("SET('a','b','c')", "'b,a,b'", "a,b"),
                Arguments.of("SET('a','b','c')", "''", ""),
                Arguments.of("SET('A','b')", "'a'", "A"),
                Arguments.of("SET('a','A') COLLATE utf8mb4_bin", "'A'", "A"),
                Arguments.of("ENUM('a','b','c')", "'B'", "b"),
                Arguments.of("ENUM('a','b','c')", "'b'", "b"),
                Arguments.of("ENUM('a','b')", "'x'", "x"),
                Arguments.of("SET('a','b')", "'a,x'", "a,x")));
    }

    private Table parseTable(String type, String literal, String action) {
        final var literalTables = new Tables();
        final String column = "v " + type + " DEFAULT " + literal;
        switch (action) {
            case "create-table" -> parser.parse("CREATE TABLE literal_defaults (id INT, " + column + ")", literalTables);
            case "add-column" -> {
                parser.parse("CREATE TABLE literal_defaults (id INT)", literalTables);
                parser.parse("ALTER TABLE literal_defaults ADD COLUMN " + column, literalTables);
            }
            case "modify-column" -> {
                // Start from another default, so that a literal that is not converted cannot keep a matching default
                parser.parse("CREATE TABLE literal_defaults (id INT, v " + type + " DEFAULT NULL)", literalTables);
                parser.parse("ALTER TABLE literal_defaults MODIFY COLUMN " + column, literalTables);
            }
            case "alter-default" -> {
                parser.parse("CREATE TABLE literal_defaults (id INT, v " + type + " DEFAULT NULL)", literalTables);
                parser.parse("ALTER TABLE literal_defaults ALTER COLUMN v SET DEFAULT " + literal, literalTables);
            }
            default -> throw new IllegalArgumentException("Unexpected DDL action: " + action);
        }
        return literalTables.forTable(new TableId(null, null, "literal_defaults"));
    }

    protected abstract P getDdlParser();

    protected abstract V getValueConverter(JdbcValueConverters.DecimalMode decimalMode,
                                           TemporalPrecisionMode temporalPrecisionMode,
                                           JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                           BinaryHandlingMode binaryHandlingMode);

    protected abstract BinlogDefaultValueConverter getDefaultValueConverter(V valueConverters);

    protected Schema getColumnSchema(Table table, String column) {
        return getColumnSchema(table, column, tableSchemaBuilder);
    }

    private Schema getColumnSchema(Table table, String column, TableSchemaBuilder tableSchemaBuilder) {
        TableSchema schema = tableSchemaBuilder.create(defaultTopicNamingStrategy(), table, null, null, null);
        return schema.getEnvelopeSchema().schema().field("after").schema().field(column).schema();
    }

    private Schema getColumnSchema(TableSchema tableSchema, String column) {
        return tableSchema.valueSchema().field(column).schema();
    }

    private DefaultTopicNamingStrategy defaultTopicNamingStrategy() {
        Properties properties = new Properties();
        properties.put("topic.prefix", "test");
        return new DefaultTopicNamingStrategy(properties);
    }
}
