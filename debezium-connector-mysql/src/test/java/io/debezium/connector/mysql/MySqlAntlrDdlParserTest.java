/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogAntlrDdlParserTest;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.charset.MySqlCharsetRegistry;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest
        extends BinlogAntlrDdlParserTest<MySqlValueConverters, MySqlDefaultValueConverter, MySqlAntlrDdlParser>
        implements MySqlCommon {
    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener) {
        return new MySqlDdlParserWithSimpleTestListener(listener);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews) {
        return new MySqlDdlParserWithSimpleTestListener(listener, includeViews);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, TableFilter tableFilter) {
        return new MySqlDdlParserWithSimpleTestListener(listener, tableFilter);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews, boolean includeComments) {
        return new MySqlDdlParserWithSimpleTestListener(listener, includeViews, includeComments);
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
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(enumValues);
    }

    @Test
    @Override
    public void parseTableWithPageChecksum() {
        // MariaDB-specific PAGE_CHECKSUM - not valid MySQL syntax
    }

    @Disabled("MySQL 5.6 system DDL has invalid timestamp defaults: '0000-00-00 00:00:00'. " +
            "Zero dates are deprecated and invalid with NO_ZERO_DATE mode (default since MySQL 5.7.4). " +
            "See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date")
    @Test
    @Override
    public void shouldParseMySql56InitializationStatements() {
    }

    @Disabled("MySQL 5.7 system DDL has invalid timestamp defaults: '0000-00-00 00:00:00'. " +
            "Zero dates are deprecated and invalid with NO_ZERO_DATE mode (default since MySQL 5.7.4). " +
            "See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date")
    @Test
    @Override
    public void shouldParseMySql57InitializationStatements() {
    }

    @Disabled("CHARACTER SET = DEFAULT syntax is not valid in MySQL 8.0+. " +
            "It was valid in MySQL 5.7 but removed in MySQL 8.0.")
    @Test
    @Override
    public void shouldProcessDefaultCharsetForTable() {
    }

    @Test
    public void testMultiColumnAlterWithDefaults() {
        String ddl = "CREATE TABLE ALTER_DATE_TIME (ID int primary key);"
                + "ALTER TABLE ALTER_DATE_TIME ADD COLUMN (CREATED timestamp not null default current_timestamp, C time not null default '08:00');";

        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "ALTER_DATE_TIME");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(3);

        Column id = table.columnWithName("ID");
        Column created = table.columnWithName("CREATED");
        Column c = table.columnWithName("C");

        assertThat(id).isNotNull();
        assertThat(created).isNotNull();
        assertThat(c).isNotNull();

        assertThat(created.typeName()).isEqualTo("TIMESTAMP");
        assertThat(created.defaultValueExpression()).isPresent();
        assertThat(created.defaultValueExpression().get()).isEqualTo("1970-01-01 00:00:00");

        assertThat(c.typeName()).isEqualTo("TIME");
        assertThat(c.defaultValueExpression()).isPresent();
        assertThat(c.defaultValueExpression().get()).isEqualTo("08:00");
    }

    @Test
    public void testCharsetIntroducerInDefault() {
        String ddl = "CREATE TABLE test_charset ("
                + "id INT PRIMARY KEY, "
                + "c1 VARCHAR(25) DEFAULT _utf8'abc', "
                + "c2 TINYINT DEFAULT _UTF8MB4'0'"
                + ");";

        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "test_charset");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(3);

        Column c1 = table.columnWithName("c1");
        Column c2 = table.columnWithName("c2");

        assertThat(c1).isNotNull();
        assertThat(c1.defaultValueExpression()).isPresent();
        assertThat(c1.defaultValueExpression().get()).isEqualTo("abc");

        assertThat(c2).isNotNull();
        assertThat(c2.defaultValueExpression()).isPresent();
        assertThat(c2.defaultValueExpression().get()).isEqualTo("0");
    }

    public static class MySqlDdlParserWithSimpleTestListener extends MySqlAntlrDdlParser {
        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener) {
            this(changesListener, false);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, TableFilter tableFilter) {
            this(changesListener, false, false, tableFilter);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews) {
            this(changesListener, includeViews, false, TableFilter.includeAll());
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments) {
            this(changesListener, includeViews, includeComments, TableFilter.includeAll());
        }

        private MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, TableFilter tableFilter) {
            super(false, includeViews, includeComments, tableFilter, new MySqlCharsetRegistry());
            this.ddlChanges = changesListener;
        }
    }
}
