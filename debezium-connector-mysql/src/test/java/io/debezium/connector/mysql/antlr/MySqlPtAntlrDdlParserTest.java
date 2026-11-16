/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

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
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
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
    protected MySqlPtAntlrDdlParser getParser(SimpleDdlParserListener listener, BinlogConnectorConfig.RealHandlingMode realHandlingMode) {
        return new MySqlPtDdlParserWithSimpleTestListener(listener, realHandlingMode);
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

    @Test
    @FixFor("debezium/dbz#2842")
    void shouldParseAlterDatabaseWithDefaultCharsetWithoutDatabaseName() {
        parser.parse("CREATE DATABASE db1 CHARACTER SET latin1;", tables);
        parser.parse("USE db1;", tables);

        parser.parse("ALTER DATABASE DEFAULT CHARACTER SET utf8mb4;", tables);
        parser.parse("USE db1;", tables);
        assertThat(parser.systemVariables().getVariable("character_set_database")).isEqualToIgnoringCase("utf8mb4");

        parser.parse("ALTER SCHEMA DEFAULT CHARSET latin2 DEFAULT COLLATE latin2_general_ci;", tables);
        parser.parse("USE db1;", tables);
        assertThat(parser.systemVariables().getVariable("character_set_database")).isEqualToIgnoringCase("latin2");
    }

    @Test
    @FixFor("debezium/dbz#2842")
    void shouldParseAlterDatabaseWithDefaultCharsetAndDatabaseName() {
        parser.parse("CREATE DATABASE db1 CHARACTER SET latin1;", tables);
        parser.parse("CREATE DATABASE db2 CHARACTER SET latin1;", tables);
        parser.parse("USE db1;", tables);

        parser.parse("ALTER DATABASE db2 DEFAULT CHARSET latin2;", tables);
        parser.parse("USE db1;", tables);
        assertThat(parser.systemVariables().getVariable("character_set_database")).isEqualToIgnoringCase("latin1");
        parser.parse("USE db2;", tables);
        assertThat(parser.systemVariables().getVariable("character_set_database")).isEqualToIgnoringCase("latin2");
    }

    @Test
    @FixFor("debezium/dbz#2217")
    public void shouldApplyRealHandlingMode() {
        final String ddl = "CREATE TABLE realtable (id INT PRIMARY KEY, r REAL);";

        final Tables doubleTables = new Tables();
        final MySqlPtAntlrDdlParser doubleParser = new MySqlPtAntlrDdlParser(
                false, false, false, TableFilter.includeAll(), new MySqlCharsetRegistry(), BinlogConnectorConfig.RealHandlingMode.DOUBLE);
        doubleParser.parse(ddl, doubleTables);
        assertThat(doubleTables.forTable(null, null, "realtable").columnWithName("r").jdbcType()).isEqualTo(Types.DOUBLE);

        final Tables floatTables = new Tables();
        final MySqlPtAntlrDdlParser floatParser = new MySqlPtAntlrDdlParser(
                false, false, false, TableFilter.includeAll(), new MySqlCharsetRegistry(), BinlogConnectorConfig.RealHandlingMode.FLOAT);
        floatParser.parse(ddl, floatTables);
        assertThat(floatTables.forTable(null, null, "realtable").columnWithName("r").jdbcType()).isEqualTo(Types.REAL);
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

    @Test
    @FixFor("debezium/dbz#3102")
    void shouldParseCastToTemporalTypeWithFractionalSecondsPrecision() {
        parser.parse("CREATE TABLE dbz3102 (id bigint primary key, status json);", tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        parser.parse("ALTER TABLE dbz3102 ADD COLUMN status_ts DATETIME(6) "
                + "GENERATED ALWAYS AS (CAST(status AS DATETIME(6))) STORED;", tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#3102")
    void shouldParseGeneratedColumnWithCastToDatetime() {
        final String ddl = "CREATE TABLE diagnosis_tasks (id bigint primary key, context_id bigint, status json);"
                + "/*app=ndb*/ ALTER TABLE diagnosis_tasks "
                + "ADD COLUMN status_state VARCHAR(64) "
                + "GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(status, '$.state'))) STORED COMMENT 'state', "
                + "ADD COLUMN status_ts DATETIME(6) "
                + "GENERATED ALWAYS AS (CAST(CONVERT_TZ("
                + "REGEXP_REPLACE(JSON_UNQUOTE(JSON_EXTRACT(status, '$.timestamp')), '[+-][0-9]{2}:[0-9]{2}$|Z$', ''), "
                + "COALESCE(REGEXP_SUBSTR(JSON_UNQUOTE(JSON_EXTRACT(status, '$.timestamp')), '[+-][0-9]{2}:[0-9]{2}$'), '+00:00'), "
                + "'+00:00') AS DATETIME(6))) STORED COMMENT 'timestamp', "
                + "ADD KEY idx_diagnosis_tasks_context_ts (context_id, status_ts DESC, id ASC), "
                + "ADD KEY idx_diagnosis_tasks_state_ts (status_state, status_ts DESC, id ASC), "
                + "ADD KEY idx_diagnosis_tasks_ts (status_ts DESC, id ASC);";

        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "diagnosis_tasks");
        assertThat(table).isNotNull();
        assertThat(table.columnWithName("status_ts")).isNotNull();
        assertThat(table.columnWithName("status_state")).isNotNull();
    }

    @Test
    @FixFor("debezium/dbz#3080")
    void shouldParseEmptyHexadecimalLiteral() {
        final String ddl = "CREATE TABLE t_default (v VARBINARY(1) DEFAULT X'');"
                + "ALTER TABLE t_default ADD COLUMN normalized_ref VARBINARY(16) GENERATED ALWAYS AS (IF(v = 'default', X'', v)) VIRTUAL;"
                + "CREATE TABLE t_control (v VARBINARY(1) DEFAULT X'00');";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.forTable(null, null, "t_default").columnWithName("normalized_ref")).isNotNull();
        assertThat(tables.forTable(null, null, "t_control").columnWithName("v")).isNotNull();
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

        MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, BinlogConnectorConfig.RealHandlingMode realHandlingMode) {
            this(changesListener, false, false, TableFilter.includeAll(), realHandlingMode);
        }

        private MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, TableFilter tableFilter) {
            this(changesListener, includeViews, includeComments, tableFilter, BinlogConnectorConfig.RealHandlingMode.DOUBLE);
        }

        private MySqlPtDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, TableFilter tableFilter,
                                                       BinlogConnectorConfig.RealHandlingMode realHandlingMode) {
            super(false, includeViews, includeComments, tableFilter, new MySqlCharsetRegistry(), realHandlingMode);
            this.ddlChanges = changesListener;
        }
    }
}
