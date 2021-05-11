/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener.Event;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest {

    private DdlParser parser;
    private Tables tables;
    private SimpleDdlParserListener listener;

    @Before
    public void beforeEach() {
        listener = new SimpleDdlParserListener();
        parser = new MysqlDdlParserWithSimpleTestListener(listener);
        tables = new Tables();
    }

    @Test
    @FixFor("DBZ-3023")
    public void shouldProcessDefaultCharsetForTable() {
        parser.parse("SET character_set_server='latin2'", tables);
        parser.parse("CREATE SCHEMA IF NOT EXISTS `database2`", tables);
        parser.parse("CREATE SCHEMA IF NOT EXISTS `database1` CHARACTER SET='windows-1250'", tables);
        parser.parse("CREATE TABLE IF NOT EXISTS `database1`.`table1` (\n"
                + "`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "`x1` VARCHAR NOT NULL\n"
                + ") CHARACTER SET = DEFAULT;", tables);
        parser.parse("CREATE TABLE IF NOT EXISTS `database2`.`table2` (\n"
                + "`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "`x1` VARCHAR NOT NULL\n"
                + ") CHARACTER SET = DEFAULT;", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(2);

        Table table = tables.forTable("database1", null, "table1");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("x1").charsetName()).isEqualTo("windows-1250");
        table = tables.forTable("database2", null, "table2");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("x1").charsetName()).isEqualTo("latin2");
    }

    @Test
    @FixFor("DBZ-3020")
    public void shouldProcessExpressionWithDefault() {
        String ddl = "create table rack_shelf_bin ( id int unsigned not null auto_increment unique primary key, bin_volume decimal(20, 4) default (bin_len * bin_width * bin_height));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "rack_shelf_bin");
        assertThat(table.columns()).hasSize(2);
        // The default value is computed for column dynamically so we set default to null
        assertThat(table.columnWithName("bin_volume").hasDefaultValue()).isTrue();
        assertThat(table.columnWithName("bin_volume").defaultValue()).isNull();
    }

    @Test
    @FixFor("DBZ-2821")
    public void shouldAllowCharacterVarying() {
        String ddl = "CREATE TABLE char_table (c1 CHAR VARYING(10), c2 CHARACTER VARYING(10), c3 NCHAR VARYING(10))";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "char_table");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("c1").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("c2").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("c3").jdbcType()).isEqualTo(Types.NVARCHAR);
    }

    @Test
    @FixFor("DBZ-2670")
    public void shouldAllowNonAsciiIdentifiers() {
        String ddl = "create table žluťoučký (kůň int);";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "žluťoučký");
        assertThat(table.columns()).hasSize(1);
        assertThat(table.columnWithName("kůň")).isNotNull();
    }

    @Test
    @FixFor("DBZ-2641")
    public void shouldProcessDimensionalBlob() {
        String ddl = "CREATE TABLE blobtable (id INT PRIMARY KEY, val1 BLOB(16), val2 BLOB);";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "blobtable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNotNull();
        assertThat(table.columnWithName("val1").length()).isEqualTo(16);
        assertThat(table.columnWithName("val2").length()).isEqualTo(-1);
    }

    @Test
    @FixFor("DBZ-2604")
    public void shouldUseDatabaseCharacterSet() {
        String ddl = "CREATE DATABASE `mydb` character set UTF8mb4 collate utf8mb4_unicode_ci;"
                + "CREATE TABLE mydb.mytable (id INT PRIMARY KEY, val1 CHAR(16) CHARSET latin2, val2 CHAR(5));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mydb.mytable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNotNull();
        assertThat(table.columnWithName("val1").charsetName()).isEqualTo("latin2");
        assertThat(table.columnWithName("val2").charsetName()).isEqualTo("UTF8mb4");
    }

    @Test
    @FixFor("DBZ-2922")
    public void shouldUseCharacterSetFromCollation() {
        String ddl = "CREATE DATABASE `sgdb` character set latin1;"
                + "CREATE TABLE sgdb.sgtable (id INT PRIMARY KEY, val1 CHAR(16) CHARSET latin2, val2 CHAR(5) collate utf8mb4_unicode_ci);";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "sgdb.sgtable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNotNull();
        assertThat(table.columnWithName("val1").charsetName()).isEqualTo("latin2");
        assertThat(table.columnWithName("val2").charsetName()).isEqualTo("utf8mb4");
    }

    @Test
    @FixFor("DBZ-2130")
    public void shouldParseCharacterDatatype() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 CHARACTER, val2 CHARACTER(5));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNotNull();
        assertThat(table.columnWithName("val1").jdbcType()).isEqualTo(Types.CHAR);
        assertThat(table.columnWithName("val1").length()).isEqualTo(-1);
        assertThat(table.columnWithName("val2").jdbcType()).isEqualTo(Types.CHAR);
        assertThat(table.columnWithName("val2").length()).isEqualTo(5);
    }

    @Test
    @FixFor("DBZ-2365")
    public void shouldParseOtherDbDatatypes() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, mi MIDDLEINT, f4 FLOAT4, f8 FLOAT8, i1 INT1, i2 INT2, i3 INT, i4 INT4, i8 INT8, l LONG CHARSET LATIN2, lvc LONG VARCHAR, lvb LONG VARBINARY);";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("mi")).isNotNull();
        assertThat(table.columnWithName("f4")).isNotNull();
        assertThat(table.columnWithName("f8")).isNotNull();
        assertThat(table.columnWithName("i1")).isNotNull();
        assertThat(table.columnWithName("i2")).isNotNull();
        assertThat(table.columnWithName("i3")).isNotNull();
        assertThat(table.columnWithName("i4")).isNotNull();
        assertThat(table.columnWithName("i8")).isNotNull();
        assertThat(table.columnWithName("mi").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(table.columnWithName("f4").jdbcType()).isEqualTo(Types.FLOAT);
        assertThat(table.columnWithName("f8").jdbcType()).isEqualTo(Types.DOUBLE);
        assertThat(table.columnWithName("i1").jdbcType()).isEqualTo(Types.SMALLINT);
        assertThat(table.columnWithName("i2").jdbcType()).isEqualTo(Types.SMALLINT);
        assertThat(table.columnWithName("i3").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(table.columnWithName("i4").jdbcType()).isEqualTo(Types.INTEGER);
        assertThat(table.columnWithName("i8").jdbcType()).isEqualTo(Types.BIGINT);
        assertThat(table.columnWithName("l").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("l").charsetName()).isEqualTo("LATIN2");
        assertThat(table.columnWithName("lvc").jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(table.columnWithName("lvb").jdbcType()).isEqualTo(Types.BLOB);
    }

    @Test
    @FixFor("DBZ-2140")
    public void shouldUpdateSchemaForRemovedDefaultValue() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 INT);"
                + "ALTER TABLE mytable ADD COLUMN last_val INT DEFAULT 5;";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        parser.parse("ALTER TABLE mytable ALTER COLUMN last_val DROP DEFAULT;", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("last_val")).isNotNull();
        assertThat(table.columnWithName("last_val").defaultValue()).isNull();

        parser.parse("ALTER TABLE mytable CHANGE COLUMN last_val last_val INT NOT NULL;", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        table = tables.forTable(null, null, "mytable");
        assertThat(table.columnWithName("last_val")).isNotNull();
        assertThat(table.columnWithName("last_val").hasDefaultValue()).isFalse();
    }

    @Test
    @FixFor("DBZ-2061")
    public void shouldUpdateSchemaForChangedDefaultValue() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 INT);"
                + "ALTER TABLE mytable ADD COLUMN last_val INT DEFAULT 5;";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        parser.parse("ALTER TABLE mytable ALTER COLUMN last_val SET DEFAULT 10;", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("last_val")).isNotNull();
        assertThat(table.columnWithName("last_val").defaultValue()).isEqualTo(10);
    }

    @Test
    @FixFor("DBZ-1833")
    public void shouldNotUpdateExistingTable() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 INT)";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        parser.parse("CREATE TABLE IF NOT EXISTS mytable (id INT PRIMARY KEY, val1 INT, val2 INT)", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNull();
    }

    @Test
    @FixFor("DBZ-1834")
    public void shouldHandleQuotes() {
        String ddl = "CREATE TABLE mytable1 (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE `mytable2` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE db.`mytable3` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE `db`.`mytable4` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE `db.mytable5` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE `db`.`myta``ble6` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE `db`.`mytable7``` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE ```db`.`mytable8` (`i.d` INT PRIMARY KEY)"
                + "CREATE TABLE ```db`.`myta\"\"ble9` (`i.d` INT PRIMARY KEY)";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(9);

        Assertions.assertThat(tables.forTable(null, null, "mytable1")).isNotNull();
        Assertions.assertThat(tables.forTable(null, null, "mytable2")).isNotNull();
        Assertions.assertThat(tables.forTable("db", null, "mytable3")).isNotNull();
        Assertions.assertThat(tables.forTable("db", null, "mytable4")).isNotNull();
        Assertions.assertThat(tables.forTable("db", null, "mytable5")).isNotNull();
        Assertions.assertThat(tables.forTable("db", null, "myta`ble6")).isNotNull();
        Assertions.assertThat(tables.forTable("db", null, "mytable7`")).isNotNull();
        Assertions.assertThat(tables.forTable("`db", null, "mytable8")).isNotNull();
        Assertions.assertThat(tables.forTable("`db", null, "myta\"\"ble9")).isNotNull();
    }

    @Test
    @FixFor("DBZ-1645")
    public void shouldUpdateAndRenameTable() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 INT, val2 INT)";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        parser.parse("ALTER TABLE mytable DROP COLUMN val1, RENAME TO newtable", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "newtable");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("val2")).isNotNull();
    }

    @Test
    @FixFor("DBZ-1560")
    public void shouldDropPrimaryKeyColumn() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, id2 INT, val INT)";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        Assertions.assertThat(table.primaryKeyColumnNames()).isEqualTo(Collections.singletonList("id"));

        parser.parse("ALTER TABLE mytable DROP COLUMN id", tables);
        table = tables.forTable(null, null, "mytable");
        Assertions.assertThat(table.primaryKeyColumnNames()).isEmpty();
        Assertions.assertThat(table.primaryKeyColumns()).isEmpty();

        parser.parse("ALTER TABLE mytable ADD PRIMARY KEY(id2)", tables);
        table = tables.forTable(null, null, "mytable");
        Assertions.assertThat(table.primaryKeyColumnNames()).isEqualTo(Collections.singletonList("id2"));
        Assertions.assertThat(table.primaryKeyColumns()).hasSize(1);
    }

    @Test
    @FixFor("DBZ-1397")
    public void shouldSupportBinaryCharset() {
        String ddl = "CREATE TABLE mytable (col BINARY(16) GENERATED ALWAYS AS (CAST('xx' as CHAR(16) CHARSET BINARY)) VIRTUAL)" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        final Table table = tables.forTable(null, null, "mytable");

        final Column f1 = table.columnWithName("col");
        assertThat(f1).isNotNull();
    }

    @Test
    @FixFor("DBZ-1376")
    public void shouldSupportCreateIndexBothAlgoAndLock() {
        parser.parse("CREATE INDEX `idx` ON `db`.`table` (created_at) COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT", tables);
        parser.parse("DROP INDEX `idx` ON `db`.`table` LOCK DEFAULT ALGORITHM DEFAULT", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-1349")
    public void shouldSupportUtfMb3Charset() {
        String ddl = " CREATE TABLE `engine_cost` (\n" +
                "  `engine_name` varchar(64) NOT NULL,\n" +
                "  `device_type` int(11) NOT NULL,\n" +
                "  `cost_name` varchar(64) NOT NULL,\n" +
                "  `cost_value` float DEFAULT NULL,\n" +
                "  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  `comment` varchar(1024) DEFAULT NULL,\n" +
                "  `default_value` float GENERATED ALWAYS AS ((case `cost_name` when _utf8mb3'io_block_read_cost' then 1.0 when _utf8mb3'memory_block_read_cost' then 0.25 else NULL end)) VIRTUAL,\n"
                +
                "  PRIMARY KEY (`cost_name`,`engine_name`,`device_type`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        final Table table = tables.forTable(null, null, "engine_cost");

        final Column f1 = table.columnWithName("default_value");
        assertThat(f1).isNotNull();
    }

    @Test
    @FixFor("DBZ-1348")
    public void shouldParseInternalColumnId() {
        String ddl = "CREATE TABLE USER (INTERNAL BOOLEAN DEFAULT FALSE) ENGINE=InnoDB DEFAULT CHARSET=latin1" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        final Table table = tables.forTable(null, null, "USER");

        final Column f1 = table.columnWithName("INTERNAL");
        assertThat(f1).isNotNull();
    }

    @Test
    public void shouldNotGetExceptionOnParseAlterStatementsWithoutCreate() {
        String ddl = "ALTER TABLE foo ADD COLUMN c bigint;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-2067")
    public void shouldSupportInstantAlgoOnAlterStatements() {
        final String ddl = "CREATE TABLE foo (id SERIAL, c1 INT);" +
                "ALTER TABLE foo ADD COLUMN c2 INT, ALGORITHM=INSTANT;";
        parser.parse(ddl, tables);

        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-1220")
    public void shouldParseFloatVariants() {
        final String ddl = "CREATE TABLE mytable (id SERIAL, f1 FLOAT, f2 FLOAT(4), f3 FLOAT(7,4));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        final Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns().size()).isEqualTo(4);

        final Column f1 = table.columnWithName("f1");
        assertThat(f1.typeName()).isEqualTo("FLOAT");
        assertThat(f1.length()).isEqualTo(-1);
        assertThat(f1.scale().isPresent()).isFalse();

        final Column f2 = table.columnWithName("f2");
        assertThat(f2.typeName()).isEqualTo("FLOAT");
        assertThat(f2.length()).isEqualTo(4);
        assertThat(f2.scale().isPresent()).isFalse();

        final Column f3 = table.columnWithName("f3");
        assertThat(f3.typeName()).isEqualTo("FLOAT");
        assertThat(f3.length()).isEqualTo(7);
        assertThat(f3.scale().get()).isEqualTo(4);
    }

    @Test
    @FixFor("DBZ-1185")
    public void shouldProcessSerialDatatype() {
        final String ddl = "CREATE TABLE foo1 (id SERIAL, val INT);" +
                "CREATE TABLE foo2 (id SERIAL PRIMARY KEY, val INT);" +
                "CREATE TABLE foo3 (id SERIAL, val INT, PRIMARY KEY(id));" +

                "CREATE TABLE foo4 (id SERIAL, val INT PRIMARY KEY);" +
                "CREATE TABLE foo5 (id SERIAL, val INT, PRIMARY KEY(val));" +

                "CREATE TABLE foo6 (id SERIAL NULL, val INT);" +

                "CREATE TABLE foo7 (id SERIAL NOT NULL, val INT);" +

                "CREATE TABLE serial (serial INT);";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        Stream.of("foo1", "foo2", "foo3").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            final Column id = table.columnWithName("id");
            assertThat(id.name()).isEqualTo("id");
            assertThat(id.typeName()).isEqualTo("BIGINT UNSIGNED");
            assertThat(id.length()).isEqualTo(-1);
            assertThat(id.isRequired()).isTrue();
            assertThat(id.isAutoIncremented()).isTrue();
            assertThat(id.isGenerated()).isTrue();
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
        });

        Stream.of("foo4", "foo5").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("val");
        });

        Stream.of("foo6").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            final Column id = table.columnWithName("id");
            assertThat(id.name()).isEqualTo("id");
            assertThat(id.typeName()).isEqualTo("BIGINT UNSIGNED");
            assertThat(id.length()).isEqualTo(-1);
            assertThat(id.isOptional()).isTrue();
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
        });

        Stream.of("foo7").forEach(tableName -> {
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
    @FixFor("DBZ-1185")
    public void shouldProcessSerialDefaultValue() {
        final String ddl = "CREATE TABLE foo1 (id SMALLINT SERIAL DEFAULT VALUE, val INT);" +
                "CREATE TABLE foo2 (id SMALLINT SERIAL DEFAULT VALUE PRIMARY KEY, val INT);" +
                "CREATE TABLE foo3 (id SMALLINT SERIAL DEFAULT VALUE, val INT, PRIMARY KEY(id));" +

                "CREATE TABLE foo4 (id SMALLINT SERIAL DEFAULT VALUE, val INT PRIMARY KEY);" +
                "CREATE TABLE foo5 (id SMALLINT SERIAL DEFAULT VALUE, val INT, PRIMARY KEY(val));" +

                "CREATE TABLE foo6 (id SMALLINT(3) NULL SERIAL DEFAULT VALUE, val INT);" +

                "CREATE TABLE foo7 (id SMALLINT(5) UNSIGNED SERIAL DEFAULT VALUE NOT NULL, val INT)";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        Stream.of("foo1", "foo2", "foo3").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            final Column id = table.columnWithName("id");
            assertThat(id.name()).isEqualTo("id");
            assertThat(id.typeName()).isEqualTo("SMALLINT");
            assertThat(id.length()).isEqualTo(-1);
            assertThat(id.isRequired()).isTrue();
            assertThat(id.isAutoIncremented()).isTrue();
            assertThat(id.isGenerated()).isTrue();
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
        });

        Stream.of("foo4", "foo5").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("val");
        });

        Stream.of("foo6").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            final Column id = table.columnWithName("id");
            assertThat(id.name()).isEqualTo("id");
            assertThat(id.typeName()).isEqualTo("SMALLINT");
            assertThat(id.length()).isEqualTo(3);
            assertThat(id.isOptional()).isTrue();
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
        });

        Stream.of("foo7").forEach(tableName -> {
            final Table table = tables.forTable(null, null, tableName);
            assertThat(table.columns().size()).isEqualTo(2);
            final Column id = table.columnWithName("id");
            assertThat(id.name()).isEqualTo("id");
            assertThat(id.typeName()).isEqualTo("SMALLINT UNSIGNED");
            assertThat(id.length()).isEqualTo(5);
            assertThat(id.isRequired()).isTrue();
            assertThat(table.primaryKeyColumnNames()).hasSize(1).containsOnly("id");
        });
    }

    @Test
    @FixFor("DBZ-1123")
    public void shouldParseGeneratedColumn() {
        String ddl = "CREATE TABLE t1 (id binary(16) NOT NULL, val char(32) GENERATED ALWAYS AS (hex(id)) STORED, PRIMARY KEY (id));"
                + "CREATE TABLE t2 (id binary(16) NOT NULL, val char(32) AS (hex(id)) STORED, PRIMARY KEY (id));"
                + "CREATE TABLE t3 (id binary(16) NOT NULL, val char(32) GENERATED ALWAYS AS (hex(id)) VIRTUAL, PRIMARY KEY (id))";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-1186")
    public void shouldParseAlterTableMultiTableOptions() {
        String ddl = "CREATE TABLE t1 (id int, PRIMARY KEY (id)) STATS_PERSISTENT=1, STATS_AUTO_RECALC=1, STATS_SAMPLE_PAGES=25;"
                + "ALTER TABLE t1 STATS_AUTO_RECALC=DEFAULT STATS_SAMPLE_PAGES=50;"
                + "ALTER TABLE t1 STATS_AUTO_RECALC=DEFAULT, STATS_SAMPLE_PAGES=50";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-1150")
    public void shouldParseCheckTableKeywords() {
        String ddl = "CREATE TABLE my_table (\n" +
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

    @Test
    @FixFor("DBZ-1233")
    public void shouldParseCheckTableSomeOtherKeyword() {
        String[] otherKeywords = new String[]{ "cache", "close", "des_key_file", "end", "export", "flush", "found",
                "general", "handler", "help", "hosts", "install", "mode", "next", "open", "relay", "reset", "slow",
                "soname", "traditional", "triggers", "uninstall", "until", "use_frm", "user_resources" };
        for (String keyword : otherKeywords) {
            String ddl = "create table t_" + keyword + "( " + keyword + " varchar(256))";
            parser.parse(ddl, tables);
            assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
            final Table table = tables.forTable(null, null, "t_" + keyword);
            assertThat(table).isNotNull();
            assertThat(table.columnWithName(keyword)).isNotNull();
        }
    }

    @Test
    @FixFor("DBZ-169")
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

    @Test
    public void shouldParseCreateStatements() {
        parser.parse(readFile("ddl/mysql-test-create.ddl"), tables);
        Testing.print(tables);
        int numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel = 50;
        assertThat(tables.size()).isEqualTo(57);
        assertThat(listener.total()).isEqualTo(144 - numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel);
    }

    @Test
    public void shouldParseTestStatements() {
        parser.parse(readFile("ddl/mysql-test-statements.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(6);
        int numberOfIndexesOnNonExistingTables = ((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size();
        int numberOfAlteredTablesWhichDoesNotExists = 10;
        // legacy parser was signaling all created index
        // antlr is parsing only those, which will make any model changes
        int numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel = 5;
        int numberOfAlterViewStatements = 6;
        // DROP VIEW statements are skipped by default
        int numberOfDroppedViews = 0;
        assertThat(listener.total()).isEqualTo(59 - numberOfAlteredTablesWhichDoesNotExists - numberOfIndexesOnNonExistingTables
                - numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel + numberOfAlterViewStatements + numberOfDroppedViews);
        listener.forEach(this::printEvent);
    }

    @Test
    public void shouldParseSomeLinesFromCreateStatements() {
        parser.parse(readLines(189, "ddl/mysql-test-create.ddl"), tables);
        assertThat(tables.size()).isEqualTo(39);
        int numberOfIndexesOnNonExistingTables = ((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size();
        int numberOfAlteredTablesWhichDoesNotExists = 2;
        int numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel = 43;
        assertThat(listener.total()).isEqualTo(120 - numberOfAlteredTablesWhichDoesNotExists
                - numberOfCreatedIndexesWhichNotMakeChangeOnTablesModel - numberOfIndexesOnNonExistingTables);
    }

    @Test
    public void shouldParseMySql56InitializationStatements() {
        parser.parse(readLines(1, "ddl/mysql-test-init-5.6.ddl"), tables);
        assertThat(tables.size()).isEqualTo(85); // 1 table
        int truncateTableStatements = 8;
        assertThat(listener.total()).isEqualTo(118 + truncateTableStatements);
        listener.forEach(this::printEvent);
    }

    @Test
    public void shouldParseMySql57InitializationStatements() {
        parser.parse(readLines(1, "ddl/mysql-test-init-5.7.ddl"), tables);
        assertThat(tables.size()).isEqualTo(123);
        int truncateTableStatements = 4;
        assertThat(listener.total()).isEqualTo(132 + truncateTableStatements);
        listener.forEach(this::printEvent);
    }

    @Test
    @FixFor("DBZ-198")
    public void shouldParseButSkipAlterTableWhenTableIsNotKnown() {
        parser.parse(readFile("ddl/mysql-dbz-198j.ddl"), tables);
        Testing.print(tables);
        listener.forEach(this::printEvent);
        assertThat(tables.size()).isEqualTo(1);

        int numberOfAlteredTablesWhichDoesNotExists = 1;
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
        assertThat(((MysqlDdlParserWithSimpleTestListener) parser).getParsingExceptionsFromWalker()).isEmpty();
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

        final String ddl = "CREATE TABLE t1 (c1 INTEGER NOT NULL,c2 VARCHAR(22),CHECK (c2 IN ('A', 'B', 'C')));"
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
        final String ddl = "CREATE TABLE t1 ("
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
        assertThat(((MysqlDdlParserWithSimpleTestListener) parser).getParsingExceptionsFromWalker()).isEmpty();
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
        String ddl = "CREATE TABLE flat_view_request_log (" +
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

    @Test
    @FixFor("DBZ-1203")
    public void parseAlterEnumColumnWithNewCharacterSet() {
        String ddl = "CREATE TABLE `test_stations_10` (\n" +
                "    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
                "    `name` varchar(500) COLLATE utf8_unicode_ci NOT NULL,\n" +
                "    `type` enum('station', 'post_office') COLLATE utf8_unicode_ci NOT NULL DEFAULT 'station',\n" +
                "    `created` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
                "    `modified` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "    PRIMARY KEY (`id`)\n" +
                ");\n" +
                "\n" +
                "ALTER TABLE `test_stations_10`\n" +
                "    MODIFY COLUMN `type` ENUM('station', 'post_office', 'plane', 'ahihi_dongok', 'now')\n" +
                "    CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci' NOT NULL DEFAULT 'station';\n";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.forTable(new TableId(null, null, "test_stations_10"))).isNotNull();
    }

    @Test
    @FixFor("DBZ-1226")
    public void parseAlterEnumColumnWithEmbeddedOrEscapedCharacters() {
        String ddl = "CREATE TABLE `test_stations_11` (\n" +
                "    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
                "    `name` varchar(500) COLLATE utf8_unicode_ci NOT NULL,\n" +
                "    `type` enum('station', 'post_office') COLLATE utf8_unicode_ci NOT NULL DEFAULT 'station',\n" +
                "    `created` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
                "    `modified` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "    PRIMARY KEY (`id`)\n" +
                ");\n" +
                "\n" +
                "ALTER TABLE `test_stations_11`\n" +
                "    MODIFY COLUMN `type` ENUM('station', 'post_office', 'plane', 'ahihi_dongok', 'now', 'a,b', 'c,\\'b')\n" +
                "    CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci' NOT NULL DEFAULT 'station';\n";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.forTable(new TableId(null, null, "test_stations_11"))).isNotNull();
    }

    @Test
    @FixFor("DBZ-1226")
    public void shouldParseEnumOptions() {
        assertParseEnumAndSetOptions("ENUM('a','b','c')", "a", "b", "c");
        assertParseEnumAndSetOptions("enum('a','b','c')", "a", "b", "c");
        assertParseEnumAndSetOptions("ENUM('a','multi','multi with () paren', 'other')", "a", "multi", "multi with () paren", "other");
        assertParseEnumAndSetOptions("ENUM('a')", "a");
        assertParseEnumAndSetOptions("ENUM ('a','b','c') CHARACTER SET utf8", "a", "b", "c");
        assertParseEnumAndSetOptions("ENUM ('a') CHARACTER SET utf8", "a");
    }

    @Test
    @FixFor({ "DBZ-476", "DBZ-1226" })
    public void shouldParseEscapedEnumOptions() {
        assertParseEnumAndSetOptions("ENUM('a''','b','c')", "a'", "b", "c");
        assertParseEnumAndSetOptions("ENUM('a\\'','b','c')", "a'", "b", "c");
        assertParseEnumAndSetOptions("ENUM(\"a\\\"\",'b','c')", "a\\\"", "b", "c");
        assertParseEnumAndSetOptions("ENUM(\"a\"\"\",'b','c')", "a\"\"", "b", "c");
    }

    @Test
    @FixFor("DBZ-1226")
    public void shouldParseSetOptions() {
        assertParseEnumAndSetOptions("SET('a','b','c')", "a", "b", "c");
        assertParseEnumAndSetOptions("set('a','b','c')", "a", "b", "c");
        assertParseEnumAndSetOptions("SET('a','multi','multi with () paren', 'other')", "a", "multi", "multi with () paren", "other");
        assertParseEnumAndSetOptions("SET('a')", "a");
        assertParseEnumAndSetOptions("SET ('a','b','c') CHARACTER SET utf8", "a", "b", "c");
        assertParseEnumAndSetOptions("SET ('a') CHARACTER SET utf8", "a");
    }

    @Test
    public void shouldParseMultipleStatements() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator()
                + "-- This is a comment" + System.lineSeparator()
                + "DROP TABLE foo;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // table created and dropped
        listener.assertNext().createTableNamed("foo").ddlStartsWith("CREATE TABLE foo (");
        listener.assertNext().dropTableNamed("foo").ddlMatches("DROP TABLE `foo`");
    }

    @Test
    public void shouldParseAlterStatementsAfterCreate() {
        String ddl1 = "CREATE TABLE foo ( c1 INTEGER NOT NULL, c2 VARCHAR(22) );" + System.lineSeparator();
        String ddl2 = "ALTER TABLE foo ADD COLUMN c bigint;" + System.lineSeparator();
        parser.parse(ddl1, tables);
        parser.parse(ddl2, tables);
        listener.assertNext().createTableNamed("foo").ddlStartsWith("CREATE TABLE foo (");
        listener.assertNext().alterTableNamed("foo").ddlStartsWith("ALTER TABLE foo ADD COLUMN c");
    }

    @Test
    public void shouldParseCreateTableStatementWithSingleGeneratedAndPrimaryKeyColumn() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + "); " + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "foo"));
        assertThat(foo).isNotNull();
        assertThat(foo.retrieveColumnNames()).containsExactly("c1", "c2");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "c1", "INTEGER", Types.INTEGER, -1, -1, false, true, true);
        assertColumn(foo, "c2", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);
    }

    @Test
    public void shouldParseCreateTableStatementWithSingleGeneratedColumnAsPrimaryKey() {
        String ddl = "CREATE TABLE my.foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " c2 VARCHAR(22), " + System.lineSeparator()
                + " PRIMARY KEY (c1)" + System.lineSeparator()
                + "); " + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId("my", null, "foo"));
        assertThat(foo).isNotNull();
        assertThat(foo.retrieveColumnNames()).containsExactly("c1", "c2");
        assertThat(foo.primaryKeyColumnNames()).containsExactly("c1");
        assertColumn(foo, "c1", "INTEGER", Types.INTEGER, -1, -1, false, true, true);
        assertColumn(foo, "c2", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);

        parser.parse("DROP TABLE my.foo", tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    public void shouldParseCreateTableStatementWithMultipleColumnsForPrimaryKey() {
        String ddl = "CREATE TABLE shop ("
                + " id BIGINT(20) NOT NULL AUTO_INCREMENT,"
                + " version BIGINT(20) NOT NULL,"
                + " name VARCHAR(255) NOT NULL,"
                + " owner VARCHAR(255) NOT NULL,"
                + " phone_number VARCHAR(255) NOT NULL,"
                + " primary key (id, name)"
                + " );";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "shop"));
        assertThat(foo).isNotNull();
        assertThat(foo.retrieveColumnNames()).containsExactly("id", "version", "name", "owner", "phone_number");
        assertThat(foo.primaryKeyColumnNames()).containsExactly("id", "name");
        assertColumn(foo, "id", "BIGINT", Types.BIGINT, 20, -1, false, true, true);
        assertColumn(foo, "version", "BIGINT", Types.BIGINT, 20, -1, false, false, false);
        assertColumn(foo, "name", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(foo, "owner", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(foo, "phone_number", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);

        parser.parse("DROP TABLE shop", tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-474")
    public void shouldParseCreateTableStatementWithCollate() {
        String ddl = "CREATE TABLE c1 (pk INT PRIMARY KEY, v1 CHAR(36) NOT NULL COLLATE utf8_unicode_ci);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(new TableId(null, null, "c1"));
        assertThat(table).isNotNull();
        assertColumn(table, "v1", "CHAR", Types.CHAR, 36, -1, false, false, false);
        Column column = table.columnWithName("v1");
        assertThat(column.typeUsesCharset()).isTrue();
    }

    @Test
    @FixFor("DBZ-646, DBZ-1398")
    public void shouldParseThirdPartyStorageEngine() {
        String ddl1 = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + ") engine=TokuDB compression=tokudb_zlib;";
        String ddl2 = "CREATE TABLE bar ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + ") engine=TokuDB compression='tokudb_zlib';";
        String ddl3 = "CREATE TABLE baz ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + ") engine=Aria;";
        String ddl4 = "CREATE TABLE escaped_foo ( " + System.lineSeparator()
                + " c1 INTEGER NOT NULL, " + System.lineSeparator()
                + " c2 VARCHAR(22) " + System.lineSeparator()
                + ") engine=TokuDB `compression`=tokudb_zlib;";
        parser.parse(ddl1 + ddl2 + ddl3 + ddl4, tables);
        assertThat(tables.size()).isEqualTo(4);
        listener.assertNext().createTableNamed("foo").ddlStartsWith("CREATE TABLE foo (");
        listener.assertNext().createTableNamed("bar").ddlStartsWith("CREATE TABLE bar (");
        listener.assertNext().createTableNamed("baz").ddlStartsWith("CREATE TABLE baz (");
        listener.assertNext().createTableNamed("escaped_foo").ddlStartsWith("CREATE TABLE escaped_foo (");
        parser.parse("DROP TABLE foo", tables);
        parser.parse("DROP TABLE bar", tables);
        parser.parse("DROP TABLE baz", tables);
        parser.parse("DROP TABLE escaped_foo", tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @FixFor("DBZ-990")
    @Test
    public void shouldParseEngineNameWithApostrophes() {
        String ddl = "CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE 'InnoDB'"
                + "CREATE TABLE t2 (id INT PRIMARY KEY) ENGINE `InnoDB`"
                + "CREATE TABLE t3 (id INT PRIMARY KEY) ENGINE \"InnoDB\""
                + "CREATE TABLE t4 (id INT PRIMARY KEY) ENGINE `RocksDB`";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(4);

        assertThat(tables.tableIds()
                .stream()
                .map(TableId::table)
                .collect(Collectors.toSet()))
                        .containsOnly("t1", "t2", "t3", "t4");
    }

    @Test
    public void shouldParseCreateUserTable() {
        String ddl = "CREATE TABLE IF NOT EXISTS user (   Host char(60) binary DEFAULT '' NOT NULL, User char(32) binary DEFAULT '' NOT NULL, Select_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Insert_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Update_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Delete_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Drop_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Reload_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Shutdown_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Process_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, File_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Grant_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, References_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Index_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_db_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Super_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tmp_table_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Lock_tables_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Execute_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_slave_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_client_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_user_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Event_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Trigger_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tablespace_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, ssl_type enum('','ANY','X509', 'SPECIFIED') COLLATE utf8_general_ci DEFAULT '' NOT NULL, ssl_cipher BLOB NOT NULL, x509_issuer BLOB NOT NULL, x509_subject BLOB NOT NULL, max_questions int(11) unsigned DEFAULT 0  NOT NULL, max_updates int(11) unsigned DEFAULT 0  NOT NULL, max_connections int(11) unsigned DEFAULT 0  NOT NULL, max_user_connections int(11) unsigned DEFAULT 0  NOT NULL, plugin char(64) DEFAULT 'mysql_native_password' NOT NULL, authentication_string TEXT, password_expired ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, password_last_changed timestamp NULL DEFAULT NULL, password_lifetime smallint unsigned NULL DEFAULT NULL, account_locked ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, PRIMARY KEY Host (Host,User) ) engine=MyISAM CHARACTER SET utf8 COLLATE utf8_bin comment='Users and global privileges';";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "user"));
        assertThat(foo).isNotNull();
        assertThat(foo.retrieveColumnNames()).contains("Host", "User", "Select_priv");
        assertColumn(foo, "Host", "CHAR BINARY", Types.BINARY, 60, -1, false, false, false);

        parser.parse("DROP TABLE user", tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    public void shouldParseCreateTableStatementWithSignedTypes() {
        String ddl = "CREATE TABLE foo ( " + System.lineSeparator()
                + " c1 BIGINT SIGNED NOT NULL, " + System.lineSeparator()
                + " c2 INT UNSIGNED NOT NULL " + System.lineSeparator()
                + "); " + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "foo"));
        assertThat(foo).isNotNull();
        assertThat(foo.retrieveColumnNames()).containsExactly("c1", "c2");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "c1", "BIGINT SIGNED", Types.BIGINT, -1, -1, false, false, false);
        assertColumn(foo, "c2", "INT UNSIGNED", Types.INTEGER, -1, -1, false, false, false);
    }

    @Test
    public void shouldParseCreateTableStatementWithCharacterSetForTable() {
        String ddl = "CREATE TABLE t ( col1 VARCHAR(25) ) DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);

        ddl = "CREATE TABLE t2 ( col1 VARCHAR(25) ) DEFAULT CHARSET utf8 DEFAULT COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table t2 = tables.forTable(new TableId(null, null, "t2"));
        assertThat(t2).isNotNull();
        assertThat(t2.retrieveColumnNames()).containsExactly("col1");
        assertThat(t2.primaryKeyColumnNames()).isEmpty();
        assertColumn(t2, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);

        ddl = "CREATE TABLE t3 ( col1 VARCHAR(25) ) CHARACTER SET utf8 COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(3);
        Table t3 = tables.forTable(new TableId(null, null, "t3"));
        assertThat(t3).isNotNull();
        assertThat(t3.retrieveColumnNames()).containsExactly("col1");
        assertThat(t3.primaryKeyColumnNames()).isEmpty();
        assertColumn(t3, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);

        ddl = "CREATE TABLE t4 ( col1 VARCHAR(25) ) CHARSET utf8 COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(4);
        Table t4 = tables.forTable(new TableId(null, null, "t4"));
        assertThat(t4).isNotNull();
        assertThat(t4.retrieveColumnNames()).containsExactly("col1");
        assertThat(t4.primaryKeyColumnNames()).isEmpty();
        assertColumn(t4, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
    }

    @Test
    public void shouldParseCreateTableStatementWithCharacterSetForColumns() {
        String ddl = "CREATE TABLE t ( col1 VARCHAR(25) CHARACTER SET greek ); ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
    }

    @Test
    public void shouldParseAlterTableStatementThatAddsCharacterSetForColumns() {
        String ddl = "CREATE TABLE t ( col1 VARCHAR(25) ); ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, null, true);

        ddl = "ALTER TABLE t MODIFY col1 VARCHAR(50) CHARACTER SET greek;";
        parser.parse(ddl, tables);
        Table t2 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t2).isNotNull();
        assertThat(t2.retrieveColumnNames()).containsExactly("col1");
        assertThat(t2.primaryKeyColumnNames()).isEmpty();
        assertColumn(t2, "col1", "VARCHAR", Types.VARCHAR, 50, "greek", true);

        ddl = "ALTER TABLE t MODIFY col1 VARCHAR(75) CHARSET utf8;";
        parser.parse(ddl, tables);
        Table t3 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t3).isNotNull();
        assertThat(t3.retrieveColumnNames()).containsExactly("col1");
        assertThat(t3.primaryKeyColumnNames()).isEmpty();
        assertColumn(t3, "col1", "VARCHAR", Types.VARCHAR, 75, "utf8", true);
    }

    @Test
    public void shouldParseCreateDatabaseAndTableThatUsesDefaultCharacterSets() {
        String ddl = "SET character_set_server=utf8;" + System.lineSeparator()
                + "CREATE DATABASE db1 CHARACTER SET utf8mb4;" + System.lineSeparator()
                + "USE db1;" + System.lineSeparator()
                + "CREATE TABLE t1 (" + System.lineSeparator()
                + " id int(11) not null auto_increment," + System.lineSeparator()
                + " c1 varchar(255) default null," + System.lineSeparator()
                + " c2 varchar(255) not null," + System.lineSeparator()
                + " c3 varchar(255) charset latin2 not null," + System.lineSeparator()
                + " primary key ('id')" + System.lineSeparator()
                + ") engine=InnoDB auto_increment=1006 default charset=latin1;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4"); // changes when we use a different database
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId("db1", null, "t1"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id", "c1", "c2", "c3");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "INT", Types.INTEGER, 11, -1, false, true, true);
        assertColumn(t, "c1", "VARCHAR", Types.VARCHAR, 255, "latin1", true);
        assertColumn(t, "c2", "VARCHAR", Types.VARCHAR, 255, "latin1", false);
        assertColumn(t, "c3", "VARCHAR", Types.VARCHAR, 255, "latin2", false);

        // Create a similar table but without a default charset for the table ...
        ddl = "CREATE TABLE t2 (" + System.lineSeparator()
                + " id int(11) not null auto_increment," + System.lineSeparator()
                + " c1 varchar(255) default null," + System.lineSeparator()
                + " c2 varchar(255) not null," + System.lineSeparator()
                + " c3 varchar(255) charset latin2 not null," + System.lineSeparator()
                + " primary key ('id')" + System.lineSeparator()
                + ") engine=InnoDB auto_increment=1006;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table t2 = tables.forTable(new TableId("db1", null, "t2"));
        assertThat(t2).isNotNull();
        assertThat(t2.retrieveColumnNames()).containsExactly("id", "c1", "c2", "c3");
        assertThat(t2.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t2, "id", "INT", Types.INTEGER, 11, -1, false, true, true);
        assertColumn(t2, "c1", "VARCHAR", Types.VARCHAR, 255, "utf8mb4", true);
        assertColumn(t2, "c2", "VARCHAR", Types.VARCHAR, 255, "utf8mb4", false);
        assertColumn(t2, "c3", "VARCHAR", Types.VARCHAR, 255, "latin2", false);
    }

    @Test
    public void shouldParseCreateDatabaseAndUseDatabaseStatementsAndHaveCharacterEncodingVariablesUpdated() {
        parser.parse("SET character_set_server=utf8;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", null);

        parser.parse("CREATE DATABASE db1 CHARACTER SET utf8mb4;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", null); // changes when we USE a different database

        parser.parse("USE db1;", tables); // changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");

        parser.parse("CREATE DATABASE db2 CHARACTER SET latin1;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");

        parser.parse("USE db2;", tables); // changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "latin1");

        parser.parse("USE db1;", tables); // changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");

        parser.parse("CREATE DATABASE db3 CHARSET latin2;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");

        parser.parse("USE db3;", tables); // changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "latin2");
    }

    @Test
    public void shouldParseSetCharacterSetStatement() {
        parser.parse("SET character_set_server=utf8;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", null);

        parser.parse("SET CHARACTER SET utf8mb4;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf8mb4");
        assertVariable("character_set_results", "utf8mb4");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", null);

        // Set the character set to the default for the current database, or since there is none then that of the server ...
        parser.parse("SET CHARACTER SET default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf8");
        assertVariable("character_set_results", "utf8");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", null);

        parser.parse("SET CHARSET utf16;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf16");
        assertVariable("character_set_results", "utf16");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", null);

        parser.parse("SET CHARSET default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf8");
        assertVariable("character_set_results", "utf8");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", null);

        parser.parse("CREATE DATABASE db1 CHARACTER SET LATIN1;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", null); // changes when we USE a different database

        parser.parse("USE db1;", tables); // changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", "LATIN1");

        parser.parse("SET CHARSET default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "LATIN1");
        assertVariable("character_set_results", "LATIN1");
        assertVariable("character_set_connection", "LATIN1");
        assertVariable("character_set_database", "LATIN1");
    }

    @Test
    public void shouldParseSetNamesStatement() {
        parser.parse("SET character_set_server=utf8;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", null);

        parser.parse("SET NAMES utf8mb4 COLLATE junk;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf8mb4");
        assertVariable("character_set_results", "utf8mb4");
        assertVariable("character_set_connection", "utf8mb4");
        assertVariable("character_set_database", null);

        // Set the character set to the default for the current database, or since there is none then that of the server ...
        parser.parse("SET NAMES default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf8");
        assertVariable("character_set_results", "utf8");
        assertVariable("character_set_connection", "utf8");
        assertVariable("character_set_database", null);

        parser.parse("SET NAMES utf16;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "utf16");
        assertVariable("character_set_results", "utf16");
        assertVariable("character_set_connection", "utf16");
        assertVariable("character_set_database", null);

        parser.parse("CREATE DATABASE db1 CHARACTER SET LATIN1;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", null); // changes when we USE a different database

        parser.parse("USE db1;", tables); // changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "LATIN1");

        parser.parse("SET NAMES default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "LATIN1");
        assertVariable("character_set_results", "LATIN1");
        assertVariable("character_set_connection", "LATIN1");
        assertVariable("character_set_database", "LATIN1");
    }

    @Test
    public void shouldParseAlterTableStatementAddColumns() {
        String ddl = "CREATE TABLE t ( col1 VARCHAR(25) ); ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
        assertThat(t.columnWithName("col1").position()).isEqualTo(1);

        ddl = "ALTER TABLE t ADD col2 VARCHAR(50) NOT NULL;";
        parser.parse(ddl, tables);
        Table t2 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t2).isNotNull();
        assertThat(t2.retrieveColumnNames()).containsExactly("col1", "col2");
        assertThat(t2.primaryKeyColumnNames()).isEmpty();
        assertColumn(t2, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
        assertColumn(t2, "col2", "VARCHAR", Types.VARCHAR, 50, -1, false, false, false);
        assertThat(t2.columnWithName("col1").position()).isEqualTo(1);
        assertThat(t2.columnWithName("col2").position()).isEqualTo(2);

        ddl = "ALTER TABLE t ADD col3 FLOAT NOT NULL AFTER col1;";
        parser.parse(ddl, tables);
        Table t3 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t3).isNotNull();
        assertThat(t3.retrieveColumnNames()).containsExactly("col1", "col3", "col2");
        assertThat(t3.primaryKeyColumnNames()).isEmpty();
        assertColumn(t3, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
        assertColumn(t3, "col3", "FLOAT", Types.FLOAT, -1, -1, false, false, false);
        assertColumn(t3, "col2", "VARCHAR", Types.VARCHAR, 50, -1, false, false, false);
        assertThat(t3.columnWithName("col1").position()).isEqualTo(1);
        assertThat(t3.columnWithName("col3").position()).isEqualTo(2);
        assertThat(t3.columnWithName("col2").position()).isEqualTo(3);
    }

    @FixFor("DBZ-660")
    @Test
    public void shouldParseAlterTableStatementAddConstraintUniqueKey() {
        String ddl = "CREATE TABLE t ( col1 VARCHAR(25) ); ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);

        ddl = "ALTER TABLE t ADD CONSTRAINT UNIQUE KEY col_key (col1);";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD CONSTRAINT UNIQUE KEY (col1);";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD UNIQUE KEY col_key (col1);";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD UNIQUE KEY (col1);";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD CONSTRAINT xx UNIQUE KEY col_key (col1);";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD CONSTRAINT xy UNIQUE KEY (col1);";
        parser.parse(ddl, tables);
    }

    @Test
    public void shouldParseCreateTableWithEnumAndSetColumns() {
        String ddl = "CREATE TABLE t ( c1 ENUM('a','b','c') NOT NULL, c2 SET('a','b','c') NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("c1", "c2");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "ENUM", Types.CHAR, 1, -1, false, false, false);
        assertColumn(t, "c2", "SET", Types.CHAR, 5, -1, true, false, false);
        assertThat(t.columnWithName("c1").position()).isEqualTo(1);
        assertThat(t.columnWithName("c2").position()).isEqualTo(2);
    }

    @Test
    public void shouldParseDefiner() {
        String function = "FUNCTION fnA( a int, b int ) RETURNS tinyint(1) begin -- anything end;";
        String ddl = "CREATE DEFINER='mysqluser'@'%' " + function;
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);

        ddl = "CREATE DEFINER='mysqluser'@'something' " + function;
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);

        ddl = "CREATE DEFINER=`mysqluser`@`something` " + function;
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);

        ddl = "CREATE DEFINER=CURRENT_USER " + function;
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);

        ddl = "CREATE DEFINER=CURRENT_USER() " + function;
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-169")
    public void shouldParseCreateAndAlterWithOnUpdate() {
        String ddl = "CREATE TABLE customers ( "
                + "id INT PRIMARY KEY NOT NULL, "
                + "name VARCHAR(30) NOT NULL, "
                + "PRIMARY KEY (id) );"
                + ""
                + "CREATE TABLE `CUSTOMERS_HISTORY` LIKE `customers`; "
                + ""
                + "ALTER TABLE `CUSTOMERS_HISTORY` MODIFY COLUMN `id` varchar(36) NOT NULL,"
                + "DROP PRIMARY KEY,"
                + "ADD action tinyint(3) unsigned NOT NULL FIRST,"
                + "ADD revision int(10) unsigned NOT NULL AFTER action,"
                + "ADD changed_on DATETIME NOT NULL DEFAULT NOW() AFTER revision,"
                + "ADD PRIMARY KEY (id, revision);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(2);
        assertThat(listener.total()).isEqualTo(3);

        Table t = tables.forTable(new TableId(null, null, "customers"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id", "name");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "INT", Types.INTEGER, -1, -1, false, false, false);
        assertColumn(t, "name", "VARCHAR", Types.VARCHAR, 30, -1, false, false, false);
        assertThat(t.columnWithName("id").position()).isEqualTo(1);
        assertThat(t.columnWithName("name").position()).isEqualTo(2);

        t = tables.forTable(new TableId(null, null, "CUSTOMERS_HISTORY"));
        assertThat(t).isNotNull();
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("action", "revision", "changed_on", "id", "name");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id", "revision");
        assertColumn(t, "action", "TINYINT UNSIGNED", Types.SMALLINT, 3, -1, false, false, false);
        assertColumn(t, "revision", "INT UNSIGNED", Types.INTEGER, 10, -1, false, false, false);
        assertColumn(t, "changed_on", "DATETIME", Types.TIMESTAMP, -1, -1, false, false, false);
        assertColumn(t, "id", "VARCHAR", Types.VARCHAR, 36, -1, false, false, false);
        assertColumn(t, "name", "VARCHAR", Types.VARCHAR, 30, -1, false, false, false);
        assertThat(t.columnWithName("action").position()).isEqualTo(1);
        assertThat(t.columnWithName("revision").position()).isEqualTo(2);
        assertThat(t.columnWithName("changed_on").position()).isEqualTo(3);
        assertThat(t.columnWithName("id").position()).isEqualTo(4);
        assertThat(t.columnWithName("name").position()).isEqualTo(5);
    }

    @Test
    @FixFor("DBZ-1411")
    public void shouldParseGrantStatement() {
        parser.parse("GRANT ALL PRIVILEGES ON `mysql`.* TO 'mysqluser'@'%'", tables);
        parser.parse("GRANT ALL PRIVILEGES ON `mysql`.`t` TO 'mysqluser'@'%'", tables);
        parser.parse("GRANT ALL PRIVILEGES ON mysql.t TO 'mysqluser'@'%'", tables);
        parser.parse("GRANT ALL PRIVILEGES ON `mysql`.t TO 'mysqluser'@'%'", tables);
        parser.parse("GRANT ALL PRIVILEGES ON mysql.`t` TO 'mysqluser'@'%'", tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);
    }

    @FixFor("DBZ-1300")
    @Test
    public void shouldParseGrantStatementWithoutSpecifiedHostName() {
        String ddl = "GRANT ALL PRIVILEGES ON `add-new-user`.* TO `add_new_user`";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    public void shouldParseSetOfOneVariableStatementWithoutTerminator() {
        String ddl = "set character_set_client=utf8";
        parser.parse(ddl, tables);
        assertVariable("character_set_client", "utf8");
    }

    @Test
    public void shouldParseSetOfOneVariableStatementWithTerminator() {
        String ddl = "set character_set_client = utf8;";
        parser.parse(ddl, tables);
        assertVariable("character_set_client", "utf8");
    }

    @Test
    public void shouldParseSetOfSameVariableWithDifferentScope() {
        String ddl = "SET GLOBAL sort_buffer_size=1000000, SESSION sort_buffer_size=1000000";
        parser.parse(ddl, tables);
        assertGlobalVariable("sort_buffer_size", "1000000");
        assertSessionVariable("sort_buffer_size", "1000000");
    }

    @Test
    public void shouldParseSetOfMultipleVariablesWithInferredScope() {
        String ddl = "SET GLOBAL v1=1, v2=2";
        parser.parse(ddl, tables);
        assertGlobalVariable("v1", "1");
        assertGlobalVariable("v2", "2");
        assertSessionVariable("v2", null);
    }

    @Test
    public void shouldParseSetOfGlobalVariable() {
        String ddl = "SET GLOBAL v1=1; SET @@global.v2=2";
        parser.parse(ddl, tables);
        assertGlobalVariable("v1", "1");
        assertGlobalVariable("v2", "2");
        assertSessionVariable("v1", null);
        assertSessionVariable("v2", null);
    }

    @Test
    public void shouldParseSetOfLocalVariable() {
        String ddl = "SET LOCAL v1=1; SET @@local.v2=2";
        parser.parse(ddl, tables);
        assertLocalVariable("v1", "1");
        assertLocalVariable("v2", "2");
        assertSessionVariable("v1", "1");
        assertSessionVariable("v2", "2");
        assertGlobalVariable("v1", null);
        assertGlobalVariable("v2", null);
    }

    @Test
    public void shouldParseSetOfSessionVariable() {
        String ddl = "SET SESSION v1=1; SET @@session.v2=2";
        parser.parse(ddl, tables);
        assertLocalVariable("v1", "1");
        assertLocalVariable("v2", "2");
        assertSessionVariable("v1", "1");
        assertSessionVariable("v2", "2");
        assertGlobalVariable("v1", null);
        assertGlobalVariable("v2", null);
    }

    @Test
    public void shouldParseButNotSetUserVariableWithUnderscoreDelimiter() {
        String ddl = "SET @a_b_c_d:=1";
        parser.parse(ddl, tables);
        assertLocalVariable("a_b_c_d", null);
        assertSessionVariable("a_b_c_d", null);
        assertGlobalVariable("a_b_c_d", null);
    }

    @Test
    public void shouldParseVariableWithUnderscoreDelimiter() {
        String ddl = "SET a_b_c_d=1";
        parser.parse(ddl, tables);
        assertSessionVariable("a_b_c_d", "1");
    }

    @Test
    public void shouldParseAndIgnoreDeleteStatements() {
        String ddl = "DELETE FROM blah";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0);
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    public void shouldParseAndIgnoreInsertStatements() {
        String ddl = "INSERT INTO blah (id) values (1)";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0);
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    public void shouldParseStatementsWithQuotedIdentifiers() {
        parser.parse(readFile("ddl/mysql-quoted.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(4);
        assertThat(listener.total()).isEqualTo(11);
        assertThat(tables.forTable("connector_test_ro", null, "products")).isNotNull();
        assertThat(tables.forTable("connector_test_ro", null, "products_on_hand")).isNotNull();
        assertThat(tables.forTable("connector_test_ro", null, "customers")).isNotNull();
        assertThat(tables.forTable("connector_test_ro", null, "orders")).isNotNull();
    }

    @Test
    public void shouldParseIntegrationTestSchema() {
        parser.parse(readFile("ddl/mysql-integration.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(10);
        assertThat(listener.total()).isEqualTo(20);
    }

    @Test
    public void shouldParseStatementForDbz106() {
        parser.parse(readFile("ddl/mysql-dbz-106.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);
    }

    @Test
    public void shouldParseStatementForDbz123() {
        parser.parse(readFile("ddl/mysql-dbz-123.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);
    }

    @FixFor("DBZ-162")
    @Test
    public void shouldParseAndIgnoreCreateFunction() {
        parser.parse(readFile("ddl/mysql-dbz-162.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1); // 1 table
        assertThat(listener.total()).isEqualTo(2); // 1 create, 1 alter
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-667")
    @Test
    public void shouldParseScientificNotationNumber() {
        String ddl = "CREATE TABLE t (id INT NOT NULL, myvalue DOUBLE DEFAULT 1E10, PRIMARY KEY (`id`));"
                + "CREATE TABLE t (id INT NOT NULL, myvalue DOUBLE DEFAULT 1.3E-10, PRIMARY KEY (`id`));"
                + "CREATE TABLE t (id INT NOT NULL, myvalue DOUBLE DEFAULT 1.4E+10, PRIMARY KEY (`id`));"
                + "CREATE TABLE t (id INT NOT NULL, myvalue DOUBLE DEFAULT 3E10, PRIMARY KEY (`id`));"
                + "CREATE TABLE t (id INT NOT NULL, myvalue DOUBLE DEFAULT 1.5e10, PRIMARY KEY (`id`))";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
    }

    @FixFor("DBZ-162")
    @Test
    public void shouldParseAlterTableWithNewlineFeeds() {
        String ddl = "CREATE TABLE `test` (id INT(11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "test"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "INT UNSIGNED", Types.INTEGER, 11, -1, false, true, true);

        ddl = "ALTER TABLE `test` CHANGE `id` `collection_id` INT(11)\n UNSIGNED\n NOT NULL\n AUTO_INCREMENT;";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        t = tables.forTable(new TableId(null, null, "test"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("collection_id");
        assertThat(t.primaryKeyColumnNames()).containsExactly("collection_id");
        assertColumn(t, "collection_id", "INT UNSIGNED", Types.INTEGER, 11, -1, false, true, true);
    }

    @FixFor("DBZ-176")
    @Test
    public void shouldParseButIgnoreCreateTriggerWithDefiner() {
        parser.parse(readFile("ddl/mysql-dbz-176.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-193")
    @Test
    public void shouldParseFulltextKeyInCreateTable() {
        parser.parse(readFile("ddl/mysql-dbz-193.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1); // 1 table
        assertThat(listener.total()).isEqualTo(1);
        listener.forEach(this::printEvent);

        Table t = tables.forTable(new TableId(null, null, "roles"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id", "name", "context", "organization_id", "client_id", "scope_action_ids");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "VARCHAR", Types.VARCHAR, 32, -1, false, false, false);
        assertColumn(t, "name", "VARCHAR", Types.VARCHAR, 100, -1, false, false, false);
        assertColumn(t, "context", "VARCHAR", Types.VARCHAR, 20, -1, false, false, false);
        assertColumn(t, "organization_id", "INT", Types.INTEGER, 11, -1, true, false, false);
        assertColumn(t, "client_id", "VARCHAR", Types.VARCHAR, 32, -1, false, false, false);
        assertColumn(t, "scope_action_ids", "TEXT", Types.VARCHAR, -1, -1, false, false, false);
        assertThat(t.columnWithName("id").position()).isEqualTo(1);
        assertThat(t.columnWithName("name").position()).isEqualTo(2);
        assertThat(t.columnWithName("context").position()).isEqualTo(3);
        assertThat(t.columnWithName("organization_id").position()).isEqualTo(4);
        assertThat(t.columnWithName("client_id").position()).isEqualTo(5);
        assertThat(t.columnWithName("scope_action_ids").position()).isEqualTo(6);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseProcedureWithCase() {
        parser.parse(readFile("ddl/mysql-dbz-198b.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @FixFor("DBZ-415")
    @Test
    public void shouldParseProcedureEmbeddedIfs() {
        parser.parse(readFile("ddl/mysql-dbz-415a.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @FixFor("DBZ-415")
    @Test
    public void shouldParseProcedureIfWithParenthesisStart() {
        parser.parse(readFile("ddl/mysql-dbz-415b.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionWithDefiner() {
        parser.parse(readFile("ddl/mysql-dbz-198a.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionC() {
        parser.parse(readFile("ddl/mysql-dbz-198c.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionD() {
        parser.parse(readFile("ddl/mysql-dbz-198d.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionE() {
        parser.parse(readFile("ddl/mysql-dbz-198e.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionF() {
        parser.parse(readFile("ddl/mysql-dbz-198f.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionG() {
        parser.parse(readFile("ddl/mysql-dbz-198g.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButIgnoreCreateFunctionH() {
        parser.parse(readFile("ddl/mysql-dbz-198h.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(0); // 0 table
        assertThat(listener.total()).isEqualTo(0);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-198")
    @Test
    public void shouldParseAlterTableWithDropIndex() {
        parser.parse(readFile("ddl/mysql-dbz-198i.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(3);
        assertThat(listener.total()).isEqualTo(4);
        listener.forEach(this::printEvent);
    }

    @Test
    @FixFor("DBZ-1329")
    public void shouldParseAlterTableWithIndex() {
        final String ddl = "USE db;"
                + "CREATE TABLE db.t1 (ID INTEGER PRIMARY KEY, val INTEGER, INDEX myidx(val));"
                + "ALTER TABLE db.t1 RENAME INDEX myidx to myidx2;";
        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        final Table table = tables.forTable(new TableId(null, "db", "t1"));
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(2);
    }

    @Test
    @FixFor("DBZ-3067")
    public void shouldParseIndex() {
        final String ddl1 = "USE db;"
                + "CREATE TABLE db.t1 (ID INTEGER PRIMARY KEY, val INTEGER, INDEX myidx(val));";
        final String ddl2 = "USE db;"
                + "CREATE OR REPLACE INDEX myidx on db.t1(val);";
        parser = new MysqlDdlParserWithSimpleTestListener(listener, true);
        parser.parse(ddl1, tables);
        assertThat(tables.size()).isEqualTo(1);
        final Table table = tables.forTable(new TableId(null, "db", "t1"));
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(2);
        parser.parse(ddl2, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @FixFor("DBZ-437")
    @Test
    public void shouldParseStringSameAsKeyword() {
        parser.parse(readFile("ddl/mysql-dbz-437.ddl"), tables);
        // Testing.Print.enable();
        listener.forEach(this::printEvent);
        assertThat(tables.size()).isEqualTo(0);
        assertThat(listener.total()).isEqualTo(0);
    }

    @FixFor("DBZ-200")
    @Test
    public void shouldParseStatementForDbz200() {
        parser.parse(readFile("ddl/mysql-dbz-200.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);

        Table t = tables.forTable(new TableId(null, null, "customfield"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("ENCODEDKEY", "ID", "CREATIONDATE", "LASTMODIFIEDDATE", "DATATYPE",
                "ISDEFAULT", "ISREQUIRED", "NAME", "VALUES", "AMOUNTS", "DESCRIPTION",
                "TYPE", "VALUELENGTH", "INDEXINLIST", "CUSTOMFIELDSET_ENCODEDKEY_OID",
                "STATE", "VALIDATIONPATTERN", "VIEWUSAGERIGHTSKEY", "EDITUSAGERIGHTSKEY",
                "BUILTINCUSTOMFIELDID", "UNIQUE", "STORAGE");
        assertColumn(t, "ENCODEDKEY", "VARCHAR", Types.VARCHAR, 32, -1, false, false, false);
        assertColumn(t, "ID", "VARCHAR", Types.VARCHAR, 32, -1, true, false, false);
        assertColumn(t, "CREATIONDATE", "DATETIME", Types.TIMESTAMP, -1, -1, true, false, false);
        assertColumn(t, "LASTMODIFIEDDATE", "DATETIME", Types.TIMESTAMP, -1, -1, true, false, false);
        assertColumn(t, "DATATYPE", "VARCHAR", Types.VARCHAR, 256, -1, true, false, false);
        assertColumn(t, "ISDEFAULT", "BIT", Types.BIT, 1, -1, true, false, false);
        assertColumn(t, "ISREQUIRED", "BIT", Types.BIT, 1, -1, true, false, false);
        assertColumn(t, "NAME", "VARCHAR", Types.VARCHAR, 256, -1, true, false, false);
        assertColumn(t, "VALUES", "MEDIUMBLOB", Types.BLOB, -1, -1, true, false, false);
        assertColumn(t, "AMOUNTS", "MEDIUMBLOB", Types.BLOB, -1, -1, true, false, false);
        assertColumn(t, "DESCRIPTION", "VARCHAR", Types.VARCHAR, 256, -1, true, false, false);
        assertColumn(t, "TYPE", "VARCHAR", Types.VARCHAR, 256, -1, true, false, false);
        assertColumn(t, "VALUELENGTH", "VARCHAR", Types.VARCHAR, 256, -1, false, false, false);
        assertColumn(t, "INDEXINLIST", "INT", Types.INTEGER, 11, -1, true, false, false);
        assertColumn(t, "CUSTOMFIELDSET_ENCODEDKEY_OID", "VARCHAR", Types.VARCHAR, 32, -1, false, false, false);
        assertColumn(t, "STATE", "VARCHAR", Types.VARCHAR, 256, -1, false, false, false);
        assertColumn(t, "VALIDATIONPATTERN", "VARCHAR", Types.VARCHAR, 256, -1, true, false, false);
        assertColumn(t, "VIEWUSAGERIGHTSKEY", "VARCHAR", Types.VARCHAR, 32, -1, true, false, false);
        assertColumn(t, "EDITUSAGERIGHTSKEY", "VARCHAR", Types.VARCHAR, 32, -1, true, false, false);
        assertColumn(t, "BUILTINCUSTOMFIELDID", "VARCHAR", Types.VARCHAR, 255, -1, true, false, false);
        assertColumn(t, "UNIQUE", "VARCHAR", Types.VARCHAR, 32, -1, false, false, false);
        assertColumn(t, "STORAGE", "VARCHAR", Types.VARCHAR, 32, -1, false, false, false);
        assertThat(t.columnWithName("ENCODEDKEY").position()).isEqualTo(1);
        assertThat(t.columnWithName("id").position()).isEqualTo(2);
        assertThat(t.columnWithName("CREATIONDATE").position()).isEqualTo(3);
        assertThat(t.columnWithName("DATATYPE").position()).isEqualTo(5);
        assertThat(t.columnWithName("UNIQUE").position()).isEqualTo(21);

    }

    @FixFor("DBZ-204")
    @Test
    public void shouldParseAlterTableThatChangesMultipleColumns() {
        String ddl = "CREATE TABLE `s`.`test` (a INT(11) NULL, b INT NULL, c INT NULL, INDEX i1(b));";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, "s", "test"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("a", "b", "c");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "a", "INT", Types.INTEGER, 11, -1, true, false, false);
        assertColumn(t, "b", "INT", Types.INTEGER, -1, -1, true, false, false);
        assertColumn(t, "c", "INT", Types.INTEGER, -1, -1, true, false, false);

        ddl = "ALTER TABLE `s`.`test` CHANGE COLUMN `a` `d` BIGINT(20) NOT NULL AUTO_INCREMENT";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        t = tables.forTable(new TableId(null, "s", "test"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("d", "b", "c");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "d", "BIGINT", Types.BIGINT, 20, -1, false, true, true);
        assertColumn(t, "b", "INT", Types.INTEGER, -1, -1, true, false, false);
        assertColumn(t, "c", "INT", Types.INTEGER, -1, -1, true, false, false);

        ddl = "ALTER TABLE `s`.`test` DROP INDEX i1";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    public void shouldParseTicketMonsterLiquibaseStatements() {
        parser.parse(readLines(1, "ddl/mysql-ticketmonster-liquibase.ddl"), tables);
        assertThat(tables.size()).isEqualTo(7);
        assertThat(listener.total()).isEqualTo(17);
        listener.forEach(this::printEvent);
    }

    @FixFor("DBZ-160")
    @Test
    public void shouldParseCreateTableWithEnumDefault() {
        String ddl = "CREATE TABLE t ( c1 ENUM('a','b','c') NOT NULL DEFAULT 'b', c2 ENUM('a', 'b', 'c') NOT NULL DEFAULT 'a');";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("c1", "c2");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "ENUM", Types.CHAR, 1, -1, false, false, false);
        assertColumn(t, "c2", "ENUM", Types.CHAR, 1, -1, false, false, false);
    }

    @FixFor("DBZ-160")
    @Test
    public void shouldParseCreateTableWithBitDefault() {
        String ddl = "CREATE TABLE t ( c1 Bit(2) NOT NULL DEFAULT b'1', c2 Bit(2) NOT NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("c1", "c2");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "BIT", Types.BIT, 2, -1, false, false, false);
        assertColumn(t, "c2", "BIT", Types.BIT, 2, -1, false, false, false);
    }

    @FixFor("DBZ-253")
    @Test
    public void shouldParseTableMaintenanceStatements() {
        String ddl = "create table `db1`.`table1` ( `id` int not null, `other` int );";
        ddl += "analyze table `db1`.`table1`;";
        ddl += "optimize table `db1`.`table1`;";
        ddl += "repair table `db1`.`table1`;";

        parser.parse(ddl, tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);
    }

    @Test
    public void shouldParseCreateTableUnionStatement() {
        final String ddl = "CREATE TABLE `merge_table` (`id` int(11) NOT NULL, `name` varchar(45) DEFAULT NULL, PRIMARY KEY (`id`)) UNION = (`table1`,`table2`) ENGINE=MRG_MyISAM DEFAULT CHARSET=latin1;";

        parser.parse(ddl, tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);
    }

    @FixFor("DBZ-346")
    @Test
    public void shouldParseAlterTableUnionStatement() {
        final String ddl = "CREATE TABLE `merge_table` (`id` int(11) NOT NULL, `name` varchar(45) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=MRG_MyISAM DEFAULT CHARSET=latin1;"
                +
                "ALTER TABLE `merge_table` UNION = (`table1`,`table2`)";

        parser.parse(ddl, tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(2);
    }

    @FixFor("DBZ-419")
    @Test
    public void shouldParseCreateTableWithUnnamedPrimaryKeyConstraint() {
        final String ddl = "CREATE TABLE IF NOT EXISTS tables_exception (table_name VARCHAR(100), create_date TIMESTAMP DEFAULT NOW(), enabled INT(1), retention int(1) default 30, CONSTRAINT PRIMARY KEY (table_name));";

        parser.parse(ddl, tables);
        Testing.print(tables);

        Table t = tables.forTable(new TableId(null, null, "tables_exception"));
        assertThat(t).isNotNull();
        assertThat(t.primaryKeyColumnNames()).containsExactly("table_name");
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    public void shouldParseStatementForDbz142() {
        parser.parse(readFile("ddl/mysql-dbz-142.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(2);
        assertThat(listener.total()).isEqualTo(2);

        Table t = tables.forTable(new TableId(null, null, "nvarchars"));
        assertColumn(t, "c1", "NVARCHAR", Types.NVARCHAR, 255, "utf8", true);
        assertColumn(t, "c2", "NATIONAL VARCHAR", Types.NVARCHAR, 255, "utf8", true);
        assertColumn(t, "c3", "NCHAR VARCHAR", Types.NVARCHAR, 255, "utf8", true);
        assertColumn(t, "c4", "NATIONAL CHARACTER VARYING", Types.NVARCHAR, 255, "utf8", true);
        assertColumn(t, "c5", "NATIONAL CHAR VARYING", Types.NVARCHAR, 255, "utf8", true);

        Table t2 = tables.forTable(new TableId(null, null, "nchars"));
        assertColumn(t2, "c1", "NATIONAL CHARACTER", Types.NCHAR, 10, "utf8", true);
        assertColumn(t2, "c2", "NCHAR", Types.NCHAR, 10, "utf8", true);
    }

    @Test
    @FixFor("DBZ-408")
    public void shouldParseCreateTableStatementWithColumnNamedColumn() {
        String ddl = "CREATE TABLE `mytable` ( " + System.lineSeparator()
                + " `def` int(11) unsigned NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " `ghi` varchar(255) NOT NULL DEFAULT '', " + System.lineSeparator()
                + " `column` varchar(255) NOT NULL DEFAULT '', " + System.lineSeparator()
                + " PRIMARY KEY (`def`) " + System.lineSeparator()
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table mytable = tables.forTable(new TableId(null, null, "mytable"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "column", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "ghi", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
    }

    @Test
    @FixFor("DBZ-428")
    public void shouldParseCreateTableWithTextType() {
        String ddl = "CREATE TABLE DBZ428 ("
                + "limtext TEXT(20), "
                + "unltext TEXT);";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table mytable = tables.forTable(new TableId(null, null, "DBZ428"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "unltext", "TEXT", Types.VARCHAR, -1, -1, true, false, false);
        assertColumn(mytable, "limtext", "TEXT", Types.VARCHAR, 20, -1, true, false, false);
    }

    @Test
    @FixFor("DBZ-439")
    public void shouldParseCreateTableWithDoublePrecisionKeyword() {
        String ddl = "CREATE TABLE DBZ439 ("
                + "limdouble DOUBLE PRECISION(20, 2),"
                + "unldouble DOUBLE PRECISION);";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table mytable = tables.forTable(new TableId(null, null, "DBZ439"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "limdouble", "DOUBLE PRECISION", Types.DOUBLE, 20, 2, true, false, false);
        assertColumn(mytable, "unldouble", "DOUBLE PRECISION", Types.DOUBLE, -1, -1, true, false, false);
    }

    @Test
    @FixFor({ "DBZ-408", "DBZ-412" })
    public void shouldParseAlterTableStatementWithColumnNamedColumnWithoutColumnWord() {
        String ddl = "CREATE TABLE `mytable` ( " + System.lineSeparator()
                + " `def` int(11) unsigned NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " PRIMARY KEY (`def`) " + System.lineSeparator()
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "ADD `column` varchar(255) NOT NULL DEFAULT '', "
                + "ADD `ghi` varchar(255) NOT NULL DEFAULT '', "
                + "ADD jkl varchar(255) NOT NULL DEFAULT '' ;";

        parser.parse(ddl, tables);

        assertThat(tables.size()).isEqualTo(1);

        Table mytable = tables.forTable(new TableId(null, null, "mytable"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "column", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "ghi", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "jkl", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);

        ddl = "ALTER TABLE `mytable` "
                + "MODIFY `column` varchar(1023) NOT NULL DEFAULT '';";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "ALTER `column` DROP DEFAULT;";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "CHANGE `column` newcol varchar(1023) NOT NULL DEFAULT '';";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "CHANGE newcol `column` varchar(255) NOT NULL DEFAULT '';";
        parser.parse(ddl, tables);

        assertThat(tables.size()).isEqualTo(1);
        mytable = tables.forTable(new TableId(null, null, "mytable"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "column", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "ghi", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "jkl", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);

        ddl = "ALTER TABLE `mytable` "
                + "DROP `column`, "
                + "DROP `ghi`, "
                + "DROP jkl";

        parser.parse(ddl, tables);
        mytable = tables.forTable(new TableId(null, null, "mytable"));
        List<String> mytableColumnNames = mytable.columns()
                .stream()
                .map(Column::name)
                .collect(Collectors.toList());

        assertThat(mytableColumnNames).containsOnly("def");
    }

    @Test
    @FixFor({ "DBZ-408", "DBZ-412", "DBZ-524" })
    public void shouldParseAlterTableStatementWithColumnNamedColumnWithColumnWord() {
        String ddl = "CREATE TABLE `mytable` ( " + System.lineSeparator()
                + " `def` int(11) unsigned NOT NULL AUTO_INCREMENT, " + System.lineSeparator()
                + " PRIMARY KEY (`def`) " + System.lineSeparator()
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "ADD COLUMN `column` varchar(255) NOT NULL DEFAULT '', "
                + "ADD COLUMN `ghi` varchar(255) NOT NULL DEFAULT '', "
                + "ADD COLUMN jkl varchar(255) NOT NULL DEFAULT '' ;";

        parser.parse(ddl, tables);

        assertThat(tables.size()).isEqualTo(1);

        Table mytable = tables.forTable(new TableId(null, null, "mytable"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "column", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "ghi", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "jkl", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);

        ddl = "ALTER TABLE `mytable` "
                + "MODIFY COLUMN `column` varchar(1023) NOT NULL DEFAULT '';";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "ALTER COLUMN `column` DROP DEFAULT;";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "CHANGE COLUMN `column` newcol varchar(1023) NOT NULL DEFAULT '';";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE `mytable` "
                + "CHANGE COLUMN newcol `column` varchar(255) NOT NULL DEFAULT '';";
        parser.parse(ddl, tables);

        assertThat(tables.size()).isEqualTo(1);
        mytable = tables.forTable(new TableId(null, null, "mytable"));
        assertThat(mytable).isNotNull();
        assertColumn(mytable, "column", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "ghi", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);
        assertColumn(mytable, "jkl", "VARCHAR", Types.VARCHAR, 255, -1, false, false, false);

        ddl = "ALTER TABLE `mytable` "
                + "DROP COLUMN `column`, "
                + "DROP COLUMN `ghi`, "
                + "DROP COLUMN jkl RESTRICT";

        parser.parse(ddl, tables);
        mytable = tables.forTable(new TableId(null, null, "mytable"));
        List<String> mytableColumnNames = mytable.columns()
                .stream()
                .map(Column::name)
                .collect(Collectors.toList());

        assertThat(mytableColumnNames).containsOnly("def");
    }

    @Test
    @FixFor("DBZ-425")
    public void shouldParseAlterTableAlterDefaultColumnValue() {
        String ddl = "CREATE TABLE t ( c1 DEC(2) NOT NULL, c2 FIXED(1,0) NOT NULL);";
        ddl += "ALTER TABLE t ALTER c1 SET DEFAULT 13;";
        parser.parse(ddl, tables);
    }

    @Test
    public void parseDdlForDecAndFixed() {
        String ddl = "CREATE TABLE t ( c1 DEC(2) NOT NULL, c2 FIXED(1,0) NOT NULL, c3 NUMERIC(3) NOT NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("c1", "c2", "c3");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "DEC", Types.DECIMAL, 2, 0, false, false, false);
        assertColumn(t, "c2", "FIXED", Types.DECIMAL, 1, 0, false, false, false);
        assertColumn(t, "c3", "NUMERIC", Types.NUMERIC, 3, 0, false, false, false);
    }

    @Test
    @FixFor({ "DBZ-615", "DBZ-727" })
    public void parseDdlForUnscaledDecAndFixed() {
        String ddl = "CREATE TABLE t ( c1 DEC NOT NULL, c2 FIXED(3) NOT NULL, c3 NUMERIC NOT NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("c1", "c2", "c3");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "DEC", Types.DECIMAL, 10, 0, false, false, false);
        assertColumn(t, "c2", "FIXED", Types.DECIMAL, 3, 0, false, false, false);
        assertColumn(t, "c3", "NUMERIC", Types.NUMERIC, 10, 0, false, false, false);
    }

    @Test
    public void parseTableWithPageChecksum() {
        String ddl = "CREATE TABLE t (id INT NOT NULL, PRIMARY KEY (`id`)) PAGE_CHECKSUM=1;" +
                "ALTER TABLE t PAGE_CHECKSUM=0;";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id");
        assertThat(t.primaryKeyColumnNames()).hasSize(1);
        assertColumn(t, "id", "INT", Types.INTEGER, -1, -1, false, false, false);
    }

    @Test
    @FixFor("DBZ-429")
    public void parseTableWithNegativeDefault() {
        String ddl = "CREATE TABLE t (id INT NOT NULL, myvalue INT DEFAULT -10, PRIMARY KEY (`id`));";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id", "myvalue");
        assertThat(t.primaryKeyColumnNames()).hasSize(1);
        assertColumn(t, "myvalue", "INT", Types.INTEGER, -1, -1, true, false, false);
    }

    @Test
    @FixFor("DBZ-475")
    public void parseUserDdlStatements() {
        String ddl = "CREATE USER 'jeffrey'@'localhost' IDENTIFIED BY 'password';"
                + "RENAME USER 'jeffrey'@'localhost' TO 'jeff'@'127.0.0.1';"
                + "DROP USER 'jeffrey'@'localhost';"
                + "SET PASSWORD FOR 'jeffrey'@'localhost' = 'auth_string';"
                + "ALTER USER 'jeffrey'@'localhost' IDENTIFIED BY 'new_password' PASSWORD EXPIRE;";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-530")
    public void parsePartitionReorganize() {
        String ddl = "CREATE TABLE flat_view_request_log (id INT NOT NULL, myvalue INT DEFAULT -10, PRIMARY KEY (`id`));"
                + "ALTER TABLE flat_view_request_log REORGANIZE PARTITION p_max INTO ( PARTITION p_2018_01_17 VALUES LESS THAN ('2018-01-17'), PARTITION p_2018_01_18 VALUES LESS THAN ('2018-01-18'), PARTITION p_max VALUES LESS THAN (MAXVALUE));";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-641")
    public void parsePartitionWithEngine() {
        String ddl = "CREATE TABLE flat_view_request_log (" +
                "  id INT NOT NULL, myvalue INT DEFAULT -10," +
                "  PRIMARY KEY (`id`)" +
                ")" +
                "ENGINE=InnoDB DEFAULT CHARSET=latin1 " +
                "PARTITION BY RANGE (to_days(`CreationDate`)) " +
                "(PARTITION p_2018_01_17 VALUES LESS THAN ('2018-01-17') ENGINE = InnoDB, " +
                "PARTITION p_2018_01_18 VALUES LESS THAN ('2018-01-18') ENGINE = InnoDB, " +
                "PARTITION p_max VALUES LESS THAN MAXVALUE ENGINE = InnoDB);";

        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.forTable(new TableId(null, null, "flat_view_request_log"))).isNotNull();
    }

    @Test
    @FixFor("DBZ-1113")
    public void parseAddMultiplePartitions() {
        String ddl = "CREATE TABLE test (id INT, PRIMARY KEY (id));"
                + "ALTER TABLE test ADD PARTITION (PARTITION p1 VALUES LESS THAN (10), PARTITION p_max VALUES LESS THAN MAXVALUE);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-767")
    public void shouldParseChangeColumnAndKeepName() {
        final String create = "CREATE TABLE test (" +
                "  id INT NOT NULL, myvalue ENUM('Foo','Bar','Baz') NOT NULL DEFAULT 'Foo'," +
                "  PRIMARY KEY (`id`)" +
                ");";

        parser.parse(create, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(new TableId(null, null, "test"));
        assertThat(table).isNotNull();
        assertThat(table.columns().size()).isEqualTo(2);

        final String alter1 = "ALTER TABLE test " +
                "  CHANGE myvalue myvalue INT;";

        parser.parse(alter1, tables);
        table = tables.forTable(new TableId(null, null, "test"));
        assertThat(table.columns().size()).isEqualTo(2);
        Column col = table.columns().get(1);
        assertThat(col.name()).isEqualTo("myvalue");
        assertThat(col.typeName()).isEqualTo("INT");

        final String alter2 = "ALTER TABLE test " +
                "  CHANGE myvalue myvalue TINYINT;";

        parser.parse(alter2, tables);
        table = tables.forTable(new TableId(null, null, "test"));
        assertThat(table.columns().size()).isEqualTo(2);
        col = table.columns().get(1);
        assertThat(col.name()).isEqualTo("myvalue");
        assertThat(col.typeName()).isEqualTo("TINYINT");
    }

    @Test
    public void parseDefaultValue() {
        String ddl = "CREATE TABLE tmp (id INT NOT NULL, " +
                "columnA CHAR(60) NOT NULL DEFAULT 'A'," +
                "columnB INT NOT NULL DEFAULT 1," +
                "columnC VARCHAR(10) NULL DEFAULT 'C'," +
                "columnD VARCHAR(10) NULL DEFAULT NULL," +
                "columnE VARCHAR(10) NOT NULL," +
                "my_dateA datetime NOT NULL DEFAULT '2018-04-27 13:28:43'," +
                "my_dateB datetime NOT NULL DEFAULT '9999-12-31');";
        parser.parse(ddl, tables);
        Table table = tables.forTable(new TableId(null, null, "tmp"));
        assertThat(table.columnWithName("id").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("columnA").defaultValue()).isEqualTo("A");
        assertThat(table.columnWithName("columnB").defaultValue()).isEqualTo(1);
        assertThat(table.columnWithName("columnC").defaultValue()).isEqualTo("C");
        assertThat(table.columnWithName("columnD").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("columnE").defaultValue()).isEqualTo(null);
        assertThat(table.columnWithName("my_dateA").defaultValue()).isEqualTo(LocalDateTime.of(2018, 4, 27, 13, 28, 43).toEpochSecond(ZoneOffset.UTC) * 1_000);
        assertThat(table.columnWithName("my_dateB").defaultValue()).isEqualTo(LocalDateTime.of(9999, 12, 31, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000);
    }

    @Test
    @FixFor("DBZ-860")
    public void shouldTreatPrimaryKeyColumnsImplicitlyAsNonNull() {
        String ddl = "CREATE TABLE data(id INT, PRIMARY KEY (id))"
                + "CREATE TABLE datadef(id INT DEFAULT 0, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));
        assertThat(table.columnWithName("id").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("id").hasDefaultValue()).isEqualTo(false);

        Table tableDef = tables.forTable(new TableId(null, null, "datadef"));
        assertThat(tableDef.columnWithName("id").isOptional()).isEqualTo(false);
        assertThat(tableDef.columnWithName("id").hasDefaultValue()).isEqualTo(true);
        assertThat(tableDef.columnWithName("id").defaultValue()).isEqualTo(0);

        ddl = "DROP TABLE IF EXISTS data; " +
                "CREATE TABLE data(id INT DEFAULT 1, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        table = tables.forTable(new TableId(null, null, "data"));
        assertThat(table.columnWithName("id").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("id").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("id").defaultValue()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-2330")
    public void shouldNotNullPositionBeforeOrAfterDefaultValue() {
        String ddl = "CREATE TABLE my_table (" +
                "ts_col TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "ts_col2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL," +
                "ts_col3 TIMESTAMP DEFAULT CURRENT_TIMESTAMP);";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "my_table"));
        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
        String isoEpoch = ZonedTimestamp.toIsoString(zdt, ZoneOffset.UTC, MySqlValueConverters::adjustTemporal);

        assertThat(table.columnWithName("ts_col").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col").defaultValue()).isEqualTo(isoEpoch);

        assertThat(table.columnWithName("ts_col2").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col2").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col2").defaultValue()).isEqualTo(isoEpoch);

        assertThat(table.columnWithName("ts_col3").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col3").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col3").defaultValue()).isNull();

        final String alter1 = "ALTER TABLE my_table " +
                " ADD ts_col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;";

        parser.parse(alter1, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columns().size()).isEqualTo(4);
        assertThat(table.columnWithName("ts_col4").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col4").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col4").defaultValue()).isEqualTo(isoEpoch);

        final String alter2 = "ALTER TABLE my_table " +
                " ADD ts_col5 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP";

        parser.parse(alter2, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columns().size()).isEqualTo(5);
        assertThat(table.columnWithName("ts_col5").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col5").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col5").defaultValue()).isEqualTo(isoEpoch);
    }

    @Test
    @FixFor("DBZ-2726")
    public void shouldParseTimestampDefaultValue() {
        // All the following default values for TIMESTAMP can be successfully applied to MySQL
        String ddl = "CREATE TABLE my_table (" +
                "ts_col01 TIMESTAMP DEFAULT '2020-01-02'," +
                "ts_col02 TIMESTAMP DEFAULT '2020-01-02 '," +
                "ts_col03 TIMESTAMP DEFAULT '2020-01-02:'," +
                "ts_col04 TIMESTAMP DEFAULT '2020-01-02--'," +
                "ts_col05 TIMESTAMP DEFAULT '2020-01-02 03'," +
                "ts_col06 TIMESTAMP DEFAULT '2020-01-02 003'," +
                "ts_col07 TIMESTAMP DEFAULT '2020-01-02 03:'," +
                "ts_col08 TIMESTAMP DEFAULT '2020-01-02 03:04'," +
                "ts_col09 TIMESTAMP DEFAULT '2020-01-02 03:004'," +
                "ts_col10 TIMESTAMP DEFAULT '2020-01-02 03:04:05'," +
                "ts_col11 TIMESTAMP(6) DEFAULT '2020-01-02 03:04:05.123456'," +
                "ts_col12 TIMESTAMP DEFAULT '2020-01-02 03:04:05.'," +
                "ts_col13 TIMESTAMP DEFAULT '2020-01-02:03:04:05'," +
                "ts_col14 TIMESTAMP DEFAULT '2020-01-02-03:04:05'," +
                "ts_col15 TIMESTAMP DEFAULT '2020-01-02--03:04:05'," +
                "ts_col16 TIMESTAMP DEFAULT '2020-01-02--03:004:0005'," +
                "ts_col17 TIMESTAMP DEFAULT '02020-0001-00002--03:004:0005'," +
                "ts_col18 TIMESTAMP DEFAULT '1970-01-01:00:00:001'," +
                "ts_col19 TIMESTAMP DEFAULT '2020-01-02 03!@#.$:{}()[]^04!@#.$:{}()[]^05'," +
                "ts_col20 TIMESTAMP DEFAULT '2020-01-02 03::04'," +
                "ts_col21 TIMESTAMP DEFAULT '2020-01-02 03::04.'," +
                "ts_col22 TIMESTAMP DEFAULT '2020-01-02 03.04'," +
                "ts_col23 TIMESTAMP DEFAULT '2020#01#02 03.04'," +
                "ts_col24 TIMESTAMP DEFAULT '2020##01--02^03.04'," +
                "ts_col25 TIMESTAMP DEFAULT '2020-01-02  03::04'" +
                ");";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "my_table"));
        assertThat(table.columnWithName("ts_col01").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col01").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col02").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col02").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col03").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col03").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col04").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col04").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col05").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col05").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:00:00"));
        assertThat(table.columnWithName("ts_col06").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col06").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:00:00"));
        assertThat(table.columnWithName("ts_col07").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col07").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:00:00"));
        assertThat(table.columnWithName("ts_col08").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col08").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col09").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col09").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col10").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col10").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col11").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col11").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05.123456"));
        assertThat(table.columnWithName("ts_col12").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col12").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col13").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col13").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col14").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col14").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col15").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col15").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col16").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col16").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col17").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col17").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col18").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col18").defaultValue()).isEqualTo(toIsoString("1970-01-01 00:00:01"));
        assertThat(table.columnWithName("ts_col19").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col19").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col20").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col20").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col21").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col21").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col22").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col22").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col23").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col23").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col24").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col24").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col25").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col25").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));

        final String alter1 = "ALTER TABLE my_table ADD ts_col TIMESTAMP DEFAULT '2020-01-02';";

        parser.parse(alter1, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columnWithName("ts_col").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));

        final String alter2 = "ALTER TABLE my_table MODIFY ts_col TIMESTAMP DEFAULT '2020-01-02:03:04:05';";

        parser.parse(alter2, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columnWithName("ts_col").hasDefaultValue()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
    }

    private String toIsoString(String timestamp) {
        return ZonedTimestamp.toIsoString(Timestamp.valueOf(timestamp).toInstant().atZone(ZoneId.systemDefault()), null);
    }

    /**
     * Assert whether the provided {@code typeExpression} string after being parsed results in a
     * list of {@code ENUM} or {@code SET} options that match exactly to the provided list of
     * {@code expecetedValues}.
     * <p/>
     * In this particular implementation, we construct a {@code CREATE} statement and invoke the
     * antlr parser on the statement defining a column named {@code options} that represents the
     * supplied {@code ENUM} or {@code SET} expression.
     *
     * @param typeExpression The {@code ENUM} or {@code SET} expression to be parsed
     * @param expectedValues An array of options expected to have been parsed from the expression.
     */
    private void assertParseEnumAndSetOptions(String typeExpression, String... expectedValues) {
        String ddl = "DROP TABLE IF EXISTS enum_set_option_test_table;" +
                "CREATE TABLE `enum_set_option_test_table` (`id` int not null auto_increment, `options` " +
                typeExpression + ", primary key(`id`));";

        parser.parse(ddl, tables);

        final Column column = tables.forTable(null, null, "enum_set_option_test_table").columnWithName("options");
        List<String> enumOptions = MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues());
        assertThat(enumOptions).contains(expectedValues);
    }

    private void assertVariable(String name, String expectedValue) {
        String actualValue = parser.systemVariables().getVariable(name);
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        }
        else {
            assertThat(actualValue).isEqualToIgnoringCase(expectedValue);
        }
    }

    private void assertVariable(SystemVariables.Scope scope, String name, String expectedValue) {
        String actualValue = parser.systemVariables().getVariable(name, scope);
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        }
        else {
            assertThat(actualValue).isEqualToIgnoringCase(expectedValue);
        }
    }

    private void assertGlobalVariable(String name, String expectedValue) {
        assertVariable(MySqlSystemVariables.MySqlScope.GLOBAL, name, expectedValue);
    }

    private void assertSessionVariable(String name, String expectedValue) {
        assertVariable(MySqlSystemVariables.MySqlScope.SESSION, name, expectedValue);
    }

    private void assertLocalVariable(String name, String expectedValue) {
        assertVariable(MySqlSystemVariables.MySqlScope.LOCAL, name, expectedValue);
    }

    private void printEvent(Event event) {
        Testing.print(event);
    }

    private String readFile(String classpathResource) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource);) {
            assertThat(stream).isNotNull();
            return IoUtil.read(stream);
        }
        catch (IOException e) {
            fail("Unable to read '" + classpathResource + "'");
        }
        assert false : "should never get here";
        return null;
    }

    /**
     * Reads the lines starting with a given line number from the specified file on the classpath. Any lines preceding the
     * given line number will be included as empty lines, meaning the line numbers will match the input file.
     *
     * @param startingLineNumber the 1-based number designating the first line to be included
     * @param classpathResource the path to the file on the classpath
     * @return the string containing the subset of the file contents; never null but possibly empty
     */
    private String readLines(int startingLineNumber, String classpathResource) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource);) {
            assertThat(stream).isNotNull();
            StringBuilder sb = new StringBuilder();
            AtomicInteger counter = new AtomicInteger();
            IoUtil.readLines(stream, line -> {
                if (counter.incrementAndGet() >= startingLineNumber) {
                    sb.append(line);
                }
                sb.append(System.lineSeparator());
            });
            return sb.toString();
        }
        catch (IOException e) {
            fail("Unable to read '" + classpathResource + "'");
        }
        assert false : "should never get here";
        return null;
    }

    private void assertColumn(Table table, String name, String typeName, int jdbcType, int length,
                              String charsetName, boolean optional) {
        Column column = table.columnWithName(name);
        assertThat(column.name()).isEqualTo(name);
        assertThat(column.typeName()).isEqualTo(typeName);
        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.length()).isEqualTo(length);
        assertThat(column.charsetName()).isEqualTo(charsetName);
        assertFalse(column.scale().isPresent());
        assertThat(column.isOptional()).isEqualTo(optional);
        assertThat(column.isGenerated()).isFalse();
        assertThat(column.isAutoIncremented()).isFalse();
    }

    private void assertColumn(Table table, String name, String typeName, int jdbcType, int length, int scale,
                              boolean optional, boolean generated, boolean autoIncremented) {
        Column column = table.columnWithName(name);
        assertThat(column.name()).isEqualTo(name);
        assertThat(column.typeName()).isEqualTo(typeName);
        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.length()).isEqualTo(length);
        if (scale == Column.UNSET_INT_VALUE) {
            assertFalse(column.scale().isPresent());
        }
        else {
            assertThat(column.scale().get()).isEqualTo(scale);
        }
        assertThat(column.isOptional()).isEqualTo(optional);
        assertThat(column.isGenerated()).isEqualTo(generated);
        assertThat(column.isAutoIncremented()).isEqualTo(autoIncremented);
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
                            TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                            JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                            BinaryHandlingMode.BYTES),
                    tableFilter);
            this.ddlChanges = changesListener;
        }
    }
}
