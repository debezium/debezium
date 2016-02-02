/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

public class MySqlDdlParserTest {

    private DdlParser parser;
    private Tables tables;

    @Before
    public void beforeEach() {
        parser = new MySqlDdlParser();
        tables = new Tables();
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
        assertThat(foo.columnNames()).containsExactly("c1", "c2");
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
        assertThat(foo.columnNames()).containsExactly("c1", "c2");
        assertThat(foo.primaryKeyColumnNames()).containsExactly("c1");
        assertColumn(foo, "c1", "INTEGER", Types.INTEGER, -1, -1, false, true, true);
        assertColumn(foo, "c2", "VARCHAR", Types.VARCHAR, 22, -1, true, false, false);

        parser.parse("DROP TABLE my.foo", tables);
        assertThat(tables.size()).isEqualTo(0);
    }
    
    @Test
    public void shouldParseCreateStatements() {
        parser.parse(readFile("ddl/mysql-test-create.ddl"), tables);
        Testing.print(tables);
    }

    @Test
    public void shouldParseTestStatements() {
        parser.parse(readFile("ddl/mysql-test-statements.ddl"), tables);
        Testing.print(tables);
    }

    @Test
    public void shouldParseSomeLinesFromCreateStatements() {
        parser.parse(readLines(189,"ddl/mysql-test-create.ddl"), tables);
    }

    protected String readFile( String classpathResource ) {
        try ( InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource); ) {
            assertThat(stream).isNotNull();
            return IoUtil.read(stream);
        } catch ( IOException e ) {
            fail("Unable to read '" + classpathResource + "'");
        }
        assert false : "should never get here";
        return null;
    }

    /**
     * Reads the lines starting with a given line number from the specified file on the classpath. Any lines preceding the
     * given line number will be included as empty lines, meaning the line numbers will match the input file.
     * @param startingLineNumber the 1-based number designating the first line to be included
     * @param classpathResource the path to the file on the classpath
     * @return the string containing the subset of the file contents; never null but possibly empty
     */
    protected String readLines( int startingLineNumber, String classpathResource ) {
        try ( InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource); ) {
            assertThat(stream).isNotNull();
            StringBuilder sb = new StringBuilder();
            AtomicInteger counter = new AtomicInteger();
            IoUtil.readLines(stream,line->{
                if (counter.incrementAndGet() >= startingLineNumber) sb.append(line);
                sb.append(System.lineSeparator());
            });
            return sb.toString();
        } catch ( IOException e ) {
            fail("Unable to read '" + classpathResource + "'");
        }
        assert false : "should never get here";
        return null;
    }

    protected void assertColumn(Table table, String name, String typeName, int jdbcType, int length, int scale,
                                boolean optional, boolean generated, boolean autoIncremented) {
        Column column = table.columnWithName(name);
        assertThat(column.name()).isEqualTo(name);
        assertThat(column.typeName()).isEqualTo(typeName);
        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.length()).isEqualTo(length);
        assertThat(column.scale()).isEqualTo(scale);
        assertThat(column.isOptional()).isEqualTo(optional);
        assertThat(column.isGenerated()).isEqualTo(generated);
        assertThat(column.isAutoIncremented()).isEqualTo(autoIncremented);
    }

}
