/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.BitDefaultValueTestCases.BitDefaultValueCase;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables.BinlogScope;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.connector.mariadb.util.MariaDbValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * @author Chris Cranford
 */
public class DefaultValueTest extends BinlogDefaultValueTest<MariaDbValueConverters, MariaDbAntlrDdlParser> {

    @ParameterizedTest(name = "{0}")
    @MethodSource("io.debezium.connector.binlog.BitDefaultValueTestCases#mariaDbNonStrictCases")
    @FixFor("debezium/dbz#2751")
    void shouldPreserveQuotedHexDefaultsWithLeadingZeroBytes(BitDefaultValueCase testCase) {
        assertBitDefaultValue(testCase);
    }

    @ParameterizedTest
    @ValueSource(strings = { "INT", "VARCHAR(8)" })
    @FixFor("debezium/dbz#2751")
    void shouldKeepBinaryLiteralDefaultsUnknownForOtherColumnTypes(String type) {
        final var tableId = new TableId(null, null, "unknown_defaults");
        parser.parse("CREATE TABLE unknown_defaults (v " + type + " DEFAULT 0b101)", tables);
        assertThat(tables.forTable(tableId).columnWithName("v").defaultValueExpression()).isEmpty();
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();

        parser.parse("ALTER TABLE unknown_defaults ALTER COLUMN v SET DEFAULT 1", tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNotNull();
        parser.parse("ALTER TABLE unknown_defaults ALTER COLUMN v SET DEFAULT 0b101", tables);
        assertThat(tables.forTable(tableId).columnWithName("v").defaultValueExpression()).isEmpty();
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2751")
    void shouldClearBitDefaultForUnknownExpression() {
        final var tableId = new TableId(null, null, "unknown_defaults");
        parser.parse("CREATE TABLE unknown_defaults (v BIT(8) DEFAULT b'1')", tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNotNull();

        parser.parse("ALTER TABLE unknown_defaults ALTER COLUMN v SET DEFAULT (1 + 1)", tables);

        assertThat(tables.forTable(tableId).columnWithName("v").defaultValueExpression()).isEmpty();
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "a", "`a`" })
    @FixFor("debezium/dbz#2751")
    void shouldKeepColumnReferenceDefaultsUnknown(String reference) {
        assertColumnReferenceDefaultsUnknown(reference);
    }

    @ParameterizedTest
    @ValueSource(strings = { "ANSI_QUOTES", "ANSI" })
    @FixFor("debezium/dbz#2751")
    void shouldKeepDoubleQuotedColumnReferenceDefaultsUnknown(String sqlMode) {
        parser.systemVariables().setVariable(BinlogScope.SESSION, "sql_mode", sqlMode);

        assertColumnReferenceDefaultsUnknown("\"a\"");
    }

    private void assertColumnReferenceDefaultsUnknown(String reference) {
        final var tableId = new TableId(null, null, "reference_defaults");
        parser.parse("CREATE TABLE reference_defaults (a BIT(8) DEFAULT b'101', v BIT(8) DEFAULT " + reference + ")", tables);
        assertThat(tables.forTable(tableId).columnWithName("v").defaultValueExpression()).isEmpty();
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();

        parser.parse("ALTER TABLE reference_defaults ALTER COLUMN v SET DEFAULT b'1'", tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNotNull();
        parser.parse("ALTER TABLE reference_defaults ALTER COLUMN v SET DEFAULT " + reference, tables);

        assertThat(tables.forTable(tableId).columnWithName("v").defaultValueExpression()).isEmpty();
        assertThat(getColumnSchema(tables.forTable(tableId), "v").defaultValue()).isNull();
    }

    @Override
    protected MariaDbAntlrDdlParser getDdlParser() {
        return new MariaDbAntlrDdlParser(true, false, true, Tables.TableFilter.includeAll(), new MariaDbCharsetRegistry());
    }

    @Override
    protected MariaDbValueConverters getValueConverter(DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode) {
        return new MariaDbValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalMode.name()),
                temporalPrecisionMode,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedMode.name()),
                binaryHandlingMode,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected BinlogDefaultValueConverter getDefaultValueConverter(MariaDbValueConverters valueConverters) {
        return new MariaDbDefaultValueConverter(valueConverters);
    }
}
