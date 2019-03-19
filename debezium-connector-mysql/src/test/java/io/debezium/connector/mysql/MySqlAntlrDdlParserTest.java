/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

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

    @Test
    @FixFor("DBZ-1185")
    public void shouldProcessSerialDatatype() {
        final String ddl =
                "CREATE TABLE foo1 (id SERIAL, val INT)" +
                "CREATE TABLE foo2 (id SERIAL PRIMARY KEY, val INT)" +
                "CREATE TABLE foo3 (id SERIAL, val INT, PRIMARY KEY(id))" +

                "CREATE TABLE foo4 (id SERIAL, val INT PRIMARY KEY)" +
                "CREATE TABLE foo5 (id SERIAL, val INT, PRIMARY KEY(val))" +

                "CREATE TABLE foo6 (id SERIAL NULL, val INT)" +

                "CREATE TABLE foo7 (id SERIAL NOT NULL, val INT)";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        Stream.of("foo1", "foo2", "foo3"
                ).forEach(tableName -> {
                    final Table table = tables.forTable(null, null, tableName);
                    assertThat(table.columns().size()).isEqualTo(2);
                    final Column id = table.columnWithName("id");
                    assertThat(id.name()).isEqualTo("id");
                    assertThat(id.typeName()).isEqualTo("BIGINT UNSIGNED");
                    assertThat(id.length()).isEqualTo(-1);
                    assertThat(id.isRequired()).isTrue();
                    assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
                });

        Stream.of("foo4", "foo5"
                ).forEach(tableName -> {
                    final Table table = tables.forTable(null, null, tableName);
                    assertThat(table.columns().size()).isEqualTo(2);
                    assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("val");
                });

        Stream.of("foo6"
                ).forEach(tableName -> {
                    final Table table = tables.forTable(null, null, tableName);
                    assertThat(table.columns().size()).isEqualTo(2);
                    final Column id = table.columnWithName("id");
                    assertThat(id.name()).isEqualTo("id");
                    assertThat(id.typeName()).isEqualTo("BIGINT UNSIGNED");
                    assertThat(id.length()).isEqualTo(-1);
                    assertThat(id.isOptional()).isTrue();
                    assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
                });

        Stream.of("foo7"
                ).forEach(tableName -> {
                    final Table table = tables.forTable(null, null, tableName);
                    assertThat(table.columns().size()).isEqualTo(2);
                    final Column id = table.columnWithName("id");
                    assertThat(id.name()).isEqualTo("id");
                    assertThat(id.typeName()).isEqualTo("BIGINT UNSIGNED");
                    assertThat(id.length()).isEqualTo(-1);
                    assertThat(id.isRequired()).isTrue();
                    assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
                });
    }

    @Test
    @FixFor("DBZ-1123")
    public void shouldParseGeneratedColumn() {
        String ddl =
                "CREATE TABLE t1 (id binary(16) NOT NULL, val char(32) GENERATED ALWAYS AS (hex(id)) STORED, PRIMARY KEY (id));"
              + "CREATE TABLE t2 (id binary(16) NOT NULL, val char(32) AS (hex(id)) STORED, PRIMARY KEY (id));"
              + "CREATE TABLE t3 (id binary(16) NOT NULL, val char(32) GENERATED ALWAYS AS (hex(id)) VIRTUAL, PRIMARY KEY (id))";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-1150")
    public void shouldParseCheckTableKeywords() {
        String ddl =
                "CREATE TABLE my_table (\n" +
                "  user_id varchar(64) NOT NULL,\n" +
                "  upgrade varchar(256),\n" +
                "  quick varchar(256),\n" +
                "  fast varchar(256),\n" +
                "  medium varchar(256),\n" +
                "  extended varchar(256),\n" +
                "  changed varchar(256),\n" +
                "  UNIQUE KEY call_states_userid (user_id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        final Table table = tables.forTable(null, null, "my_table");
        assertThat(table.columnWithName("upgrade")).isNotNull();
        assertThat(table.columnWithName("quick")).isNotNull();
        assertThat(table.columnWithName("fast")).isNotNull();
        assertThat(table.columnWithName("medium")).isNotNull();
        assertThat(table.columnWithName("extended")).isNotNull();
        assertThat(table.columnWithName("changed")).isNotNull();
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
        assertThat(t.retrieveColumnNames()).containsExactly("c1", "c2", "c3", "c4");
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
        assertThat(foo.retrieveColumnNames()).containsExactly("c1", "c2");
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
    @FixFor("DBZ-1059")
    public void shouldParseAlterTableRename() {
        final String ddl = "USE db;"
                  + "CREATE TABLE db.t1 (ID INTEGER PRIMARY KEY);"
                  + "ALTER TABLE `t1` RENAME TO `t2`;"
                  + "ALTER TABLE `db`.`t2` RENAME TO `db`.`t3`;";
        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        final Table table = tables.forTable(new TableId(null, "db", "t3"));
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(1);
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
        assertThat(foo.retrieveColumnNames()).containsExactly("w1");
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
        assertThat(foo.retrieveColumnNames()).containsExactly("w1");
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
        assertThat(foo.retrieveColumnNames()).containsExactly("c2");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "c2", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);
    }

    @Test
    public void shouldUseFiltersForAlterTable() {
        parser = new MysqlDdlParserWithSimpleTestListener(listener, TableFilter.fromPredicate(x -> !x.table().contains("ignored")));

        final String ddl = "CREATE TABLE ok (id int primary key, val smallint);" + System.lineSeparator()
                + "ALTER TABLE ignored ADD COLUMN(x tinyint)" + System.lineSeparator()
                + "ALTER TABLE ok ADD COLUMN(y tinyint)";
        parser.parse(ddl, tables);
        assertThat(((MysqlDdlParserWithSimpleTestListener)parser).getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.size()).isEqualTo(1);

        final Table t1 = tables.forTable(null, null, "ok");
        assertThat(t1.columns()).hasSize(3);

        final Column c1 = t1.columns().get(0);
        final Column c2 = t1.columns().get(1);
        final Column c3 = t1.columns().get(2);
        assertThat(c1.name()).isEqualTo("id");
        assertThat(c1.typeName()).isEqualTo("INT");
        assertThat(c2.name()).isEqualTo("val");
        assertThat(c2.typeName()).isEqualTo("SMALLINT");
        assertThat(c3.name()).isEqualTo("y");
        assertThat(c3.typeName()).isEqualTo("TINYINT");
    }

    @Test
    @FixFor("DBZ-903")
    public void shouldParseFunctionNamedDatabase() {
        parser = new MysqlDdlParserWithSimpleTestListener(listener, TableFilter.fromPredicate(x -> !x.table().contains("ignored")));

        final String ddl = "SELECT `table_name` FROM `information_schema`.`TABLES` WHERE `table_schema` = DATABASE()";
        parser.parse(ddl, tables);
    }

    @Test
    @FixFor("DBZ-910")
    public void shouldParseConstraintCheck() {
        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);

        final String ddl =
                "CREATE TABLE t1 (c1 INTEGER NOT NULL,c2 VARCHAR(22),CHECK (c2 IN ('A', 'B', 'C')));"
              + "CREATE TABLE t2 (c1 INTEGER NOT NULL,c2 VARCHAR(22),CONSTRAINT c1 CHECK (c2 IN ('A', 'B', 'C')));"
              + "CREATE TABLE t3 (c1 INTEGER NOT NULL,c2 VARCHAR(22),CONSTRAINT CHECK (c2 IN ('A', 'B', 'C')));"
              + "ALTER TABLE t1 ADD CONSTRAINT CHECK (c1 IN (1, 2, 3, 4));"
              + "ALTER TABLE t1 ADD CONSTRAINT c2 CHECK (c1 IN (1, 2, 3, 4))"
              + "ALTER TABLE t1 ADD CHECK (c1 IN (1, 2, 3, 4))";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(3);

        assertThat(tables.forTable(null, null, "t1").columns()).hasSize(2);
        assertThat(tables.forTable(null, null, "t2").columns()).hasSize(2);
        assertThat(tables.forTable(null, null, "t3").columns()).hasSize(2);
    }

    @Test
    @FixFor("DBZ-1028")
    public void shouldParseCommentWithEngineName() {
        final String ddl =
                "CREATE TABLE t1 ("
                + "`id` int(11) NOT NULL AUTO_INCREMENT, "
                + "`field_1` int(11) NOT NULL,  "
                + "`field_2` int(11) NOT NULL,  "
                + "`field_3` int(11) NOT NULL,  "
                + "`field_4` int(11) NOT NULL,  "
                + "`field_5` tinytext COLLATE utf8_unicode_ci NOT NULL, "
                + "`field_6` tinytext COLLATE utf8_unicode_ci NOT NULL, "
                + "`field_7` tinytext COLLATE utf8_unicode_ci NOT NULL COMMENT 'CSV',"
                + "primary key(id));";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);

        Column columnWithComment = tables.forTable(null, null, "t1").columnWithName("field_7");
        assertThat(columnWithComment.typeName()).isEqualToIgnoringCase("tinytext");
    }

    @Test
    @FixFor("DBZ-780")
    public void shouldRenameColumnWithoutDefinition() {
        parser = new MysqlDdlParserWithSimpleTestListener(listener, TableFilter.fromPredicate(x -> !x.table().contains("ignored")));

        final String ddl = "CREATE TABLE foo (id int primary key, old INT);" + System.lineSeparator()
                + "ALTER TABLE foo RENAME COLUMN old to new ";
        parser.parse(ddl, tables);
        assertThat(((MysqlDdlParserWithSimpleTestListener)parser).getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.size()).isEqualTo(1);

        final Table t1 = tables.forTable(null, null, "foo");
        assertThat(t1.columns()).hasSize(2);

        final Column c1 = t1.columns().get(0);
        final Column c2 = t1.columns().get(1);
        assertThat(c1.name()).isEqualTo("id");
        assertThat(c1.typeName()).isEqualTo("INT");
        assertThat(c2.name()).isEqualTo("new");
        assertThat(c2.typeName()).isEqualTo("INT");
    }

    @Test
    @FixFor("DBZ-959")
    public void parseAddPartition() {
        String ddl =
                "CREATE TABLE flat_view_request_log (" +
                "  id INT NOT NULL, myvalue INT DEFAULT -10," +
                "  PRIMARY KEY (`id`)" +
                ")" +
                "ENGINE=InnoDB DEFAULT CHARSET=latin1 " +
                "PARTITION BY RANGE (to_days(`CreationDate`)) " +
                "(PARTITION p_2018_01_17 VALUES LESS THAN ('2018-01-17') ENGINE = InnoDB, " +
                "PARTITION p_2018_01_18 VALUES LESS THAN ('2018-01-18') ENGINE = InnoDB, " +
                "PARTITION p_max VALUES LESS THAN MAXVALUE ENGINE = InnoDB);"
            + "ALTER TABLE flat_view_request_log ADD PARTITION (PARTITION p201901 VALUES LESS THAN (737425) ENGINE = InnoDB);";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.forTable(new TableId(null, null, "flat_view_request_log"))).isNotNull();
    }

    @Test
    @FixFor("DBZ-688")
    public void parseGeomCollection() {
        String ddl = "CREATE TABLE geomtable (id int(11) PRIMARY KEY, collection GEOMCOLLECTION DEFAULT NULL)";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.forTable(new TableId(null, null, "geomtable"))).isNotNull();
    }

    @Override
    protected void assertParseEnumAndSetOptions(String typeExpression, String optionString) {
        List<String> options = MySqlAntlrDdlParser.parseSetAndEnumOptions(typeExpression);
        String commaSeperatedOptions = Strings.join(",", options);
        assertThat(optionString).isEqualTo(commaSeperatedOptions);
    }

    class MysqlDdlParserWithSimpleTestListener extends MySqlAntlrDdlParser {

        public MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener) {
            this(changesListener, false);
        }

        public MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, TableFilter tableFilter) {
            this(changesListener, false, tableFilter);
        }

        public MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews) {
            this(changesListener, includeViews, TableFilter.includeAll());
        }

        private MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, TableFilter tableFilter) {
            super(false,
                    includeViews,
                    new MySqlValueConverters(
                        JdbcValueConverters.DecimalMode.DOUBLE,
                        TemporalPrecisionMode.ADAPTIVE,
                        JdbcValueConverters.BigIntUnsignedMode.PRECISE
                    ),
                    tableFilter);
            this.ddlChanges = changesListener;
        }
    }
}
