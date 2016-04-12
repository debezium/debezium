/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

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
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

public class MySqlDdlParserTest {
    
    private DdlParser parser;
    private Tables tables;
    private SimpleDdlParserListener listener;

    @Before
    public void beforeEach() {
        parser = new MySqlDdlParser();
        tables = new Tables();
        listener = new SimpleDdlParserListener();
        parser.addListener(listener);
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
        listener.assertNext().dropTableNamed("foo").ddlMatches("DROP TABLE foo");
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
    public void shouldParseAlterStatementsWithoutCreate() {
        String ddl = "ALTER TABLE foo ADD COLUMN c bigint;" + System.lineSeparator();
        parser.parse(ddl, tables);
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
    public void shouldParseCreateUserTable() {
        String ddl = "CREATE TABLE IF NOT EXISTS user (   Host char(60) binary DEFAULT '' NOT NULL, User char(32) binary DEFAULT '' NOT NULL, Select_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Insert_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Update_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Delete_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Drop_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Reload_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Shutdown_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Process_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, File_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Grant_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, References_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Index_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_db_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Super_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tmp_table_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Lock_tables_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Execute_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_slave_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_client_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_user_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Event_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Trigger_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tablespace_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, ssl_type enum('','ANY','X509', 'SPECIFIED') COLLATE utf8_general_ci DEFAULT '' NOT NULL, ssl_cipher BLOB NOT NULL, x509_issuer BLOB NOT NULL, x509_subject BLOB NOT NULL, max_questions int(11) unsigned DEFAULT 0  NOT NULL, max_updates int(11) unsigned DEFAULT 0  NOT NULL, max_connections int(11) unsigned DEFAULT 0  NOT NULL, max_user_connections int(11) unsigned DEFAULT 0  NOT NULL, plugin char(64) DEFAULT 'mysql_native_password' NOT NULL, authentication_string TEXT, password_expired ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, password_last_changed timestamp NULL DEFAULT NULL, password_lifetime smallint unsigned NULL DEFAULT NULL, account_locked ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, PRIMARY KEY Host (Host,User) ) engine=MyISAM CHARACTER SET utf8 COLLATE utf8_bin comment='Users and global privileges';";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "user"));
        assertThat(foo).isNotNull();
        assertThat(foo.columnNames()).contains("Host", "User", "Select_priv");
        assertColumn(foo, "Host", "CHAR BINARY", Types.BLOB, 60, -1, false, false, false);

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
        assertThat(foo.columnNames()).containsExactly("c1", "c2");
        assertThat(foo.primaryKeyColumnNames()).isEmpty();
        assertColumn(foo, "c1", "BIGINT SIGNED", Types.BIGINT, -1, -1, false, false, false);
        assertColumn(foo, "c2", "INT UNSIGNED", Types.INTEGER, -1, -1, false, false, false);
    }

    @Test
    public void shouldParseGrantStatement() {
        String ddl = "GRANT ALL PRIVILEGES ON `mysql`.* TO 'mysqluser'@'%'";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0); // no tables
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    public void shouldParseCreateStatements() {
        parser.parse(readFile("ddl/mysql-test-create.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(57); // no tables
        assertThat(listener.total()).isEqualTo(144);
    }

    @Test
    public void shouldParseTestStatements() {
        parser.parse(readFile("ddl/mysql-test-statements.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(6); // no tables
        assertThat(listener.total()).isEqualTo(46);
    }

    @Test
    public void shouldParseSomeLinesFromCreateStatements() {
        parser.parse(readLines(189,"ddl/mysql-test-create.ddl"), tables);
        assertThat(tables.size()).isEqualTo(39); // no tables
        assertThat(listener.total()).isEqualTo(120);
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
