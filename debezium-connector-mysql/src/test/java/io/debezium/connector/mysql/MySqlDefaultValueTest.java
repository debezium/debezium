/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.connector.binlog.jdbc.BinlogSystemVariables.CHARSET_NAME_CLIENT;
import static io.debezium.connector.binlog.jdbc.BinlogSystemVariables.CHARSET_NAME_CONNECTION;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables.BinlogScope;
import io.debezium.connector.mysql.BitDefaultValueTestCases.BitDefaultValueCase;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * @author laomei
 */
public class MySqlDefaultValueTest extends BinlogDefaultValueTest<MySqlValueConverters, MySqlAntlrDdlParser> {

    @ParameterizedTest(name = "{0}")
    @MethodSource({ "io.debezium.connector.mysql.BitDefaultValueTestCases#cases", "io.debezium.connector.mysql.BitDefaultValueTestCases#nonStrictCases" })
    @FixFor("debezium/dbz#2751")
    void shouldPreserveBitDefaultValues(BitDefaultValueCase testCase) {
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
                parser.parse("CREATE TABLE bit_defaults (id INT PRIMARY KEY, " + columnType + " DEFAULT b'1')", tables);
                ddl = "ALTER TABLE bit_defaults MODIFY COLUMN " + columnDefinition;
                break;
            case "alter-default":
                parser.parse("CREATE TABLE bit_defaults (id INT PRIMARY KEY, " + columnType + " DEFAULT b'1')", tables);
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

    @Override
    protected MySqlAntlrDdlParser getDdlParser() {
        return new MySqlAntlrDdlParser();
    }

    @Override
    protected MySqlValueConverters getValueConverter(JdbcValueConverters.DecimalMode decimalMode,
                                                     TemporalPrecisionMode temporalPrecisionMode,
                                                     JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                                     BinaryHandlingMode binaryHandlingMode) {
        return new MySqlValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalMode.name()),
                temporalPrecisionMode,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedMode.name()),
                binaryHandlingMode,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected BinlogDefaultValueConverter getDefaultValueConverter(MySqlValueConverters valueConverters) {
        return new MySqlDefaultValueConverter(valueConverters);
    }
}
