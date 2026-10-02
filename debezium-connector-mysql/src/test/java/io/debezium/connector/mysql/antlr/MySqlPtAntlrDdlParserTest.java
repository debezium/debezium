/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogAntlrDdlParserTest;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.MySqlCommon;
import io.debezium.connector.mysql.charset.MySqlCharsetRegistry;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * Tests for the legacy Positive Technologies MySQL DDL parser ({@link MySqlPtAntlrDdlParser}).
 */
public class MySqlPtAntlrDdlParserTest
        extends BinlogAntlrDdlParserTest<MySqlValueConverters, MySqlDefaultValueConverter, MySqlPtAntlrDdlParser>
        implements MySqlCommon {

    @Override
    protected MySqlPtAntlrDdlParser getParser(SimpleDdlParserListener listener) {
        return new MySqlPtDdlParserWithSimpleTestListener(listener);
    }

    @Override
    protected MySqlPtAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews) {
        return new MySqlPtDdlParserWithSimpleTestListener(listener, includeViews);
    }

    @Override
    protected MySqlPtAntlrDdlParser getParser(SimpleDdlParserListener listener, TableFilter tableFilter) {
        return new MySqlPtDdlParserWithSimpleTestListener(listener, tableFilter);
    }

    @Override
    protected MySqlPtAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews, boolean includeComments) {
        return new MySqlPtDdlParserWithSimpleTestListener(listener, includeViews, includeComments);
    }

    @Override
    protected MySqlValueConverters getValueConverters() {
        return new MySqlValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(JdbcValueConverters.DecimalMode.DOUBLE.name()),
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(JdbcValueConverters.BigIntUnsignedMode.PRECISE.name()),
                BinaryHandlingMode.BYTES,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected MySqlDefaultValueConverter getDefaultValueConverters(MySqlValueConverters valueConverters) {
        return new MySqlDefaultValueConverter(valueConverters);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(List<String> enumValues) {
        return MySqlPtAntlrDdlParser.extractEnumAndSetOptions(enumValues);
    }

    @ParameterizedTest
    @CsvSource(value = { "0,0", "0.0,0", "0e0,0", "'0000',0", "'0',2000", "'00',2000" }, quoteCharacter = '"')
    @FixFor("debezium/dbz#2757")
    void shouldPreserveZeroYearDefaults(String literal, int expected) {
        final var tableId = new TableId(null, null, "year_defaults");
        final var converters = getDefaultValueConverters(getValueConverters());
        parser.parse("CREATE TABLE year_defaults (y YEAR DEFAULT " + literal + ")", tables);
        final var created = tables.forTable(tableId).columnWithName("y");
        assertThat(converters.parseDefaultValue(created, created.defaultValueExpression().orElse(null))).contains(expected);

        parser.parse("ALTER TABLE year_defaults ALTER COLUMN y SET DEFAULT 2026", tables);
        parser.parse("ALTER TABLE year_defaults ALTER COLUMN y SET DEFAULT " + literal, tables);
        final var altered = tables.forTable(tableId).columnWithName("y");
        assertThat(converters.parseDefaultValue(altered, altered.defaultValueExpression().orElse(null))).contains(expected);
    }

    @Test
    @FixFor("debezium/dbz#2321")
    void shouldParseCreateProcedureWithForUpdateSkipLocked() {
        String ddl = "CREATE DEFINER=`someuser`@`localhost` PROCEDURE `GetRowsForUpdate`(\n" +
                "  IN inLimit INT\n" +
                ")\n" +
                "BEGIN\n" +
                "  SELECT ID\n" +
                "  FROM SomeTable\n" +
                "  WHERE bActive = 1\n" +
                "  ORDER BY CreatedAt ASC\n" +
                "  LIMIT inLimit\n" +
                "  FOR UPDATE SKIP LOCKED;\n" +
                "END";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2321")
    void shouldParseCreateProcedureWithForUpdateNowait() {
        String ddl = "CREATE DEFINER=`someuser`@`localhost` PROCEDURE `GetRowsNowait`(\n" +
                "  IN inLimit INT\n" +
                ")\n" +
                "BEGIN\n" +
                "  SELECT ID\n" +
                "  FROM SomeTable\n" +
                "  WHERE bActive = 1\n" +
                "  ORDER BY CreatedAt ASC\n" +
                "  LIMIT inLimit\n" +
                "  FOR UPDATE NOWAIT;\n" +
                "END";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2321")
    void shouldParseCreateProcedureWithPlainForUpdate() {
        String ddl = "CREATE DEFINER=`someuser`@`localhost` PROCEDURE `GetRowsPlain`(\n" +
                "  IN inLimit INT\n" +
                ")\n" +
                "BEGIN\n" +
                "  SELECT ID\n" +
                "  FROM SomeTable\n" +
                "  WHERE bActive = 1\n" +
                "  ORDER BY CreatedAt ASC\n" +
                "  LIMIT inLimit\n" +
                "  FOR UPDATE;\n" +
                "END";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.size()).isEqualTo(0);
    }

    public static class MySqlPtDdlParserWithSimpleTestListener extends MySqlPtAntlrDdlParser {
        MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener) {
            this(changesListener, false);
        }

        MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, TableFilter tableFilter) {
            this(changesListener, false, false, tableFilter);
        }

        MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews) {
            this(changesListener, includeViews, false, TableFilter.includeAll());
        }

        MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments) {
            this(changesListener, includeViews, includeComments, TableFilter.includeAll());
        }

        private MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, TableFilter tableFilter) {
            super(false, includeViews, includeComments, tableFilter, new MySqlCharsetRegistry());
            this.ddlChanges = changesListener;
        }
    }
}
