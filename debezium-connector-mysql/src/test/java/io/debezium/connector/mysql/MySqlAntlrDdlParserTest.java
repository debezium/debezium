/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.util.Strings;
import io.debezium.util.Testing;
import org.junit.Test;

import java.sql.Types;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest extends MySqlDdlParserTest {

    @Override
    public void beforeEach() {
        listener = new SimpleDdlParserListener();
        parser = new MysqlDdlParserWithSimpleTestListener(listener);
        tables = new Tables();
    }

    @Test
    public void shouldGetExceptionOnParseAlterStatementsWithoutCreate() {
        String ddl = "ALTER TABLE foo ADD COLUMN c bigint;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(1);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Override
    public void shouldParseAlterStatementsWithoutCreate() {
        // ignore this test - antlr equivalent for it is shouldGetExceptionOnParseAlterStatementsWithoutCreate test
    }

    @Override
    public void shouldParseTimeWithNowDefault() {
        String ddl = "CREATE TABLE t1 ( "
                + "c1 int primary key auto_increment, "
                + "c2 datetime, "
                + "c3 datetime on update now(), "
                + "c4 char(4));";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t1"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("c1", "c2", "c3", "c4");
        assertThat(t.primaryKeyColumnNames()).containsExactly("c1");
        assertColumn(t, "c1", "INT", Types.INTEGER, -1, -1, false, true, true);
        assertColumn(t, "c2", "DATETIME", Types.TIMESTAMP, -1, -1, true, false, false);
        assertColumn(t, "c3", "DATETIME", Types.TIMESTAMP, -1, -1, true, true, true);
        assertColumn(t, "c4", "CHAR", Types.CHAR, 4, -1, true, false, false);
        assertThat(t.columnWithName("c1").position()).isEqualTo(1);
        assertThat(t.columnWithName("c2").position()).isEqualTo(2);
        assertThat(t.columnWithName("c3").position()).isEqualTo(3);
        assertThat(t.columnWithName("c4").position()).isEqualTo(4);
    }

    @Override
    public void shouldParseCreateStatements() {
        parser.parse(readFile("ddl/mysql-test-create.ddl"), tables);
        Testing.print(tables);
        int numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel = 49;
        assertThat(tables.size()).isEqualTo(57);
        assertThat(listener.total()).isEqualTo(144 - numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel);
    }

    @Override
    public void shouldParseTestStatements() {
        parser.parse(readFile("ddl/mysql-test-statements-fixed.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(6);
        int numberOfAlteredTablesWhichDoesNotExists = ((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size();
        // legacy parser was signaling all created index
        // antlr is parsing only those, which will make any model changes
        int numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel = 5;
        int numberOfAlterViewStatements = 6;
        int numberOfDroppedViews = 7;
        assertThat(listener.total()).isEqualTo(58 - numberOfAlteredTablesWhichDoesNotExists
                - numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel + numberOfAlterViewStatements + numberOfDroppedViews);
        listener.forEach(this::printEvent);
    }

    @Override
    public void shouldParseSomeLinesFromCreateStatements() {
        parser.parse(readLines(189, "ddl/mysql-test-create.ddl"), tables);
        assertThat(tables.size()).isEqualTo(39);
        int numberOfAlteredTablesWhichDoesNotExists = ((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size();
        int numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel = 42;
        assertThat(listener.total()).isEqualTo(120 - numberOfAlteredTablesWhichDoesNotExists
                - numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel);
    }

    @Override
    public void shouldParseMySql56InitializationStatements() {
        parser.parse(readLines(1, "ddl/mysql-test-init-5.6.ddl"), tables);
        assertThat(tables.size()).isEqualTo(85); // 1 table
        int truncateTableStatements = 8;
        assertThat(listener.total()).isEqualTo(118 + truncateTableStatements);
        listener.forEach(this::printEvent);
    }

    @Override
    public void shouldParseMySql57InitializationStatements() {
        parser.parse(readLines(1, "ddl/mysql-test-init-5.7.ddl"), tables);
        assertThat(tables.size()).isEqualTo(123);
        int truncateTableStatements = 4;
        assertThat(listener.total()).isEqualTo(132 + truncateTableStatements);
        listener.forEach(this::printEvent);
    }

    @Override
    public void shouldParseButSkipAlterTableWhenTableIsNotKnown() {
        parser.parse(readFile("ddl/mysql-dbz-198j.ddl"), tables);
        Testing.print(tables);
        listener.forEach(this::printEvent);
        assertThat(tables.size()).isEqualTo(1);

        int numberOfAlteredTablesWhichDoesNotExists = ((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size();
        assertThat(listener.total()).isEqualTo(2 - numberOfAlteredTablesWhichDoesNotExists);
    }

    @Test
    public void shouldParseTruncateStatementsAfterCreate() {
        String ddl1 = "CREATE TABLE foo ( c1 INTEGER NOT NULL, c2 VARCHAR(22) );" + System.lineSeparator();
        String ddl2 = "TRUNCATE TABLE foo" + System.lineSeparator();
        parser.parse(ddl1, tables);
        parser.parse(ddl2, tables);
        listener.assertNext().createTableNamed("foo").ddlStartsWith("CREATE TABLE foo (");
        listener.assertNext().truncateTableNamed("foo").ddlStartsWith("TRUNCATE TABLE foo");
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    public void shouldParseCreateViewStatementStartSelect() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator();
        String ddl2 = "CREATE VIEW fooView AS (SELECT * FROM foo)" + System.lineSeparator();

        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        parser.parse(ddl2, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table foo = tables.forTable(new TableId(null, null, "fooView"));
        assertThat(foo).isNotNull();
        assertThat(foo.columnNames()).containsExactly("c1", "c2");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "c1", "INTEGER", Types.INTEGER, -1, -1, false, true, true);
        assertColumn(foo, "c2", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);
    }

    @Test
    public void shouldParseDropView() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator();
        String ddl2 = "CREATE VIEW fooView AS (SELECT * FROM foo)" + System.lineSeparator();
        String ddl3 = "DROP VIEW fooView";
        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        parser.parse(ddl2, tables);
        parser.parse(ddl3, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "fooView"));
        assertThat(foo).isNull();
    }

    @Test
    public void shouldParseCreateViewStatementColumnAlias() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator();
        String ddl2 = "CREATE VIEW fooView(w1) AS (SELECT c2 as w1 FROM foo)" + System.lineSeparator();

        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        parser.parse(ddl2, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table foo = tables.forTable(new TableId(null, null, "fooView"));
        assertThat(foo).isNotNull();
        assertThat(foo.columnNames()).containsExactly("w1");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "w1", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);
    }

    @Test
    public void shouldParseCreateViewStatementColumnAliasInnerSelect() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator();
        String ddl2 = "CREATE VIEW fooView(w1) AS (SELECT foo2.c2 as w1 FROM (SELECT c1 as c2 FROM foo) AS foo2)" + System.lineSeparator();

        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        parser.parse(ddl2, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table foo = tables.forTable(new TableId(null, null, "fooView"));
        assertThat(foo).isNotNull();
        assertThat(foo.columnNames()).containsExactly("w1");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "w1", "INTEGER", Types.INTEGER, -1, -1, false, true, true);
    }

    @Test
    public void shouldParseAlterViewStatementColumnAliasInnerSelect() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator();
        String ddl2 = "CREATE VIEW fooView(w1) AS (SELECT foo2.c2 as w1 FROM (SELECT c1 as c2 FROM foo) AS foo2)" + System.lineSeparator();
        String ddl3 = "ALTER VIEW fooView AS (SELECT c2 FROM foo)";
        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        parser.parse(ddl2, tables);
        parser.parse(ddl3, tables);
        assertThat(tables.size()).isEqualTo(2);
        assertThat(listener.total()).isEqualTo(3);
        Table foo = tables.forTable(new TableId(null, null, "fooView"));
        assertThat(foo).isNotNull();
        assertThat(foo.columnNames()).containsExactly("c2");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "c2", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);
    }

    protected void assertParseEnumAndSetOptions(String typeExpression, String optionString) {
        List<String> options = MySqlAntlrDdlParser.parseSetAndEnumOptions(typeExpression);
        String commaSeperatedOptions = Strings.join(",", options);
        assertThat(optionString).isEqualTo(commaSeperatedOptions);
    }

    class MysqlDdlParserWithSimpleTestListener extends MySqlAntlrDdlParser {
        public MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener) {
            this(changesListener, false);
        }

        public MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews) {
            super(false, includeViews);
            this.ddlChanges = changesListener;
        }
    }
}
