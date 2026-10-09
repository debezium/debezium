/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
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

    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '"', textBlock = """
            CHAR(16)           | 'old'
            VARCHAR(16)        | 'old'
            BINARY(8)          | 'old'
            VARBINARY(16)      | 'old'
            ENUM('old','null') | 'old'
            SET('old','null')  | 'old'
            BOOL               | TRUE
            BOOLEAN            | TRUE
            BIT(1)             | b'1'
            BIT(8)             | b'1'
            BIT(64)            | b'1'
            TINYINT            | 1
            SMALLINT           | 1
            MEDIUMINT          | 1
            INT                | 1
            BIGINT             | 1
            BIGINT UNSIGNED    | 1
            DECIMAL(12,2)      | 1.25
            FLOAT              | 1.25
            DOUBLE             | 1.25
            REAL               | 1.25
            DATE               | '2026-01-01'
            TIME(6)            | '01:02:03.123456'
            DATETIME(6)        | '2026-01-01 01:02:03.123456'
            TIMESTAMP(6)       | '2026-01-01 01:02:03.123456'
            YEAR               | 2026
            """)
    @FixFor("debezium/dbz#2755")
    void shouldClearDefaultValueWhenAlteredToNull(String type, String defaultValue) {
        for (String initialDefault : List.of(" DEFAULT " + defaultValue, " DEFAULT NULL", "")) {
            for (String alteration : List.of("ALTER COLUMN v SET DEFAULT NULL", "ALTER v SET DEFAULT null")) {
                parser.parse("CREATE TABLE null_defaults (v " + type + " NULL" + initialDefault + ")", tables);
                parser.parse("ALTER TABLE null_defaults " + alteration, tables);

                final var table = tables.forTable(new TableId(null, null, "null_defaults"));
                assertThat(table.columnWithName("v").defaultValueExpression())
                        .as("%s%s: %s", type, initialDefault, alteration).isEmpty();
                assertThat(getColumnSchema(table, "v").defaultValue()).isNull();
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2755")
    void shouldDistinguishNullDefaultFromStringLiteral() {
        parser.parse("CREATE TABLE null_defaults (v VARCHAR(16) DEFAULT 'old')", tables);
        final var tableId = new TableId(null, null, "null_defaults");

        parser.parse("ALTER TABLE null_defaults ALTER COLUMN v SET DEFAULT 'null'", tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isEqualTo("null");

        parser.parse("ALTER TABLE null_defaults ALTER COLUMN v SET DEFAULT NULL", tables);
        assertThat(tables.forTable(tableId).columnWithName("v").defaultValueExpression()).isEmpty();
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();

        parser.parse("ALTER TABLE null_defaults ALTER COLUMN v SET DEFAULT 'null'", tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isEqualTo("null");

        parser.parse("ALTER TABLE null_defaults ALTER COLUMN v DROP DEFAULT", tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();
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
