/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParserListener.Event;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.util.IoUtil;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

public class MySqlDdlParserTest {

    private MySqlDdlParser parser;
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
        assertThat(foo.columnNames()).containsExactly("id", "version", "name", "owner", "phone_number");
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
    public void shouldParseCreateUserTable() {
        String ddl = "CREATE TABLE IF NOT EXISTS user (   Host char(60) binary DEFAULT '' NOT NULL, User char(32) binary DEFAULT '' NOT NULL, Select_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Insert_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Update_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Delete_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Drop_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Reload_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Shutdown_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Process_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, File_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Grant_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, References_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Index_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_db_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Super_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tmp_table_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Lock_tables_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Execute_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_slave_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_client_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_user_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Event_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Trigger_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tablespace_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, ssl_type enum('','ANY','X509', 'SPECIFIED') COLLATE utf8_general_ci DEFAULT '' NOT NULL, ssl_cipher BLOB NOT NULL, x509_issuer BLOB NOT NULL, x509_subject BLOB NOT NULL, max_questions int(11) unsigned DEFAULT 0  NOT NULL, max_updates int(11) unsigned DEFAULT 0  NOT NULL, max_connections int(11) unsigned DEFAULT 0  NOT NULL, max_user_connections int(11) unsigned DEFAULT 0  NOT NULL, plugin char(64) DEFAULT 'mysql_native_password' NOT NULL, authentication_string TEXT, password_expired ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, password_last_changed timestamp NULL DEFAULT NULL, password_lifetime smallint unsigned NULL DEFAULT NULL, account_locked ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, PRIMARY KEY Host (Host,User) ) engine=MyISAM CHARACTER SET utf8 COLLATE utf8_bin comment='Users and global privileges';";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "user"));
        assertThat(foo).isNotNull();
        assertThat(foo.columnNames()).contains("Host", "User", "Select_priv");
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
        assertThat(foo.columnNames()).containsExactly("c1", "c2");
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
        assertThat(t.columnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);

        ddl = "CREATE TABLE t2 ( col1 VARCHAR(25) ) DEFAULT CHARSET utf8 DEFAULT COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table t2 = tables.forTable(new TableId(null, null, "t2"));
        assertThat(t2).isNotNull();
        assertThat(t2.columnNames()).containsExactly("col1");
        assertThat(t2.primaryKeyColumnNames()).isEmpty();
        assertColumn(t2, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);

        ddl = "CREATE TABLE t3 ( col1 VARCHAR(25) ) CHARACTER SET utf8 COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(3);
        Table t3 = tables.forTable(new TableId(null, null, "t3"));
        assertThat(t3).isNotNull();
        assertThat(t3.columnNames()).containsExactly("col1");
        assertThat(t3.primaryKeyColumnNames()).isEmpty();
        assertColumn(t3, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);

        ddl = "CREATE TABLE t4 ( col1 VARCHAR(25) ) CHARSET utf8 COLLATE utf8_general_ci; ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(4);
        Table t4 = tables.forTable(new TableId(null, null, "t4"));
        assertThat(t4).isNotNull();
        assertThat(t4.columnNames()).containsExactly("col1");
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
        assertThat(t.columnNames()).containsExactly("col1");
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
        assertThat(t.columnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, null, true);

        ddl = "ALTER TABLE t MODIFY col1 VARCHAR(50) CHARACTER SET greek;";
        parser.parse(ddl, tables);
        Table t2 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t2).isNotNull();
        assertThat(t2.columnNames()).containsExactly("col1");
        assertThat(t2.primaryKeyColumnNames()).isEmpty();
        assertColumn(t2, "col1", "VARCHAR", Types.VARCHAR, 50, "greek", true);

        ddl = "ALTER TABLE t MODIFY col1 VARCHAR(75) CHARSET utf8;";
        parser.parse(ddl, tables);
        Table t3 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t3).isNotNull();
        assertThat(t3.columnNames()).containsExactly("col1");
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
                + " c2 varchar(255) charset default not null," + System.lineSeparator()
                + " c3 varchar(255) charset latin2 not null," + System.lineSeparator()
                + " primary key ('id')" + System.lineSeparator()
                + ") engine=InnoDB auto_increment=1006 default charset=latin1;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4"); // changes when we use a different database
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId("db1", null, "t1"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("id", "c1", "c2", "c3");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "INT", Types.INTEGER, 11, -1, false, true, true);
        assertColumn(t, "c1", "VARCHAR", Types.VARCHAR, 255, "latin1", true);
        assertColumn(t, "c2", "VARCHAR", Types.VARCHAR, 255, "latin1", false);
        assertColumn(t, "c3", "VARCHAR", Types.VARCHAR, 255, "latin2", false);

        // Create a similar table but without a default charset for the table ...
        ddl = "CREATE TABLE t2 (" + System.lineSeparator()
                + " id int(11) not null auto_increment," + System.lineSeparator()
                + " c1 varchar(255) default null," + System.lineSeparator()
                + " c2 varchar(255) charset default not null," + System.lineSeparator()
                + " c3 varchar(255) charset latin2 not null," + System.lineSeparator()
                + " primary key ('id')" + System.lineSeparator()
                + ") engine=InnoDB auto_increment=1006;" + System.lineSeparator();
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(2);
        Table t2 = tables.forTable(new TableId("db1", null, "t2"));
        assertThat(t2).isNotNull();
        assertThat(t2.columnNames()).containsExactly("id", "c1", "c2", "c3");
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

        parser.parse("USE db1;", tables);// changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");

        parser.parse("CREATE DATABASE db2 CHARACTER SET latin1;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");

        parser.parse("USE db2;", tables);// changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "latin1");

        parser.parse("USE db1;", tables);// changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "utf8mb4");
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

        parser.parse("CREATE DATABASE db1 CHARACTER SET cs1;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", null); // changes when we USE a different database

        parser.parse("USE db1;", tables);// changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "cs1");

        parser.parse("SET CHARSET default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "cs1");
        assertVariable("character_set_results", "cs1");
        assertVariable("character_set_connection", null);
        assertVariable("character_set_database", "cs1");
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

        parser.parse("CREATE DATABASE db1 CHARACTER SET cs1;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", null); // changes when we USE a different database

        parser.parse("USE db1;", tables);// changes the "character_set_database" system variable ...
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_database", "cs1");

        parser.parse("SET NAMES default;", tables);
        assertVariable("character_set_server", "utf8");
        assertVariable("character_set_client", "cs1");
        assertVariable("character_set_results", "cs1");
        assertVariable("character_set_connection", "cs1");
        assertVariable("character_set_database", "cs1");
    }

    @Test
    public void shouldParseAlterTableStatementAddColumns() {
        String ddl = "CREATE TABLE t ( col1 VARCHAR(25) ); ";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("col1");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
        assertThat(t.columnWithName("col1").position()).isEqualTo(1);

        ddl = "ALTER TABLE t ADD col2 VARCHAR(50) NOT NULL;";
        parser.parse(ddl, tables);
        Table t2 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t2).isNotNull();
        assertThat(t2.columnNames()).containsExactly("col1", "col2");
        assertThat(t2.primaryKeyColumnNames()).isEmpty();
        assertColumn(t2, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
        assertColumn(t2, "col2", "VARCHAR", Types.VARCHAR, 50, -1, false, false, false);
        assertThat(t2.columnWithName("col1").position()).isEqualTo(1);
        assertThat(t2.columnWithName("col2").position()).isEqualTo(2);

        ddl = "ALTER TABLE t ADD col3 FLOAT NOT NULL AFTER col1;";
        parser.parse(ddl, tables);
        Table t3 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t3).isNotNull();
        assertThat(t3.columnNames()).containsExactly("col1", "col3", "col2");
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

        ddl = "ALTER TABLE t ADD CONSTRAINT UNIQUE KEY col_key ('col1');";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD CONSTRAINT UNIQUE KEY ('col1');";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD UNIQUE KEY col_key ('col1');";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD UNIQUE KEY ('col1');";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD CONSTRAINT 'xx' UNIQUE KEY col_key ('col1');";
        parser.parse(ddl, tables);

        ddl = "ALTER TABLE t ADD CONSTRAINT 'xx' UNIQUE KEY ('col1');";
        parser.parse(ddl, tables);
    }


    @Test
    public void shouldParseCreateTableWithEnumAndSetColumns() {
        String ddl = "CREATE TABLE t ( c1 ENUM('a','b','c') NOT NULL, c2 SET('a','b','c') NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("c1", "c2");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "ENUM", Types.CHAR, 1, -1, false, false, false);
        assertColumn(t, "c2", "SET", Types.CHAR, 5, -1, true, false, false);
        assertThat(t.columnWithName("c1").position()).isEqualTo(1);
        assertThat(t.columnWithName("c2").position()).isEqualTo(2);
    }

    @Test
    public void shouldParseDefiner() {
        String function = "FUNCTION fnA( a int, b int ) RETURNS tinyint(1) begin anything end;";
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
        assertColumn(t, "c3", "DATETIME", Types.TIMESTAMP, -1, -1, true, false, true);
        assertColumn(t, "c4", "CHAR", Types.CHAR, 4, -1, true, false, false);
        assertThat(t.columnWithName("c1").position()).isEqualTo(1);
        assertThat(t.columnWithName("c2").position()).isEqualTo(2);
        assertThat(t.columnWithName("c3").position()).isEqualTo(3);
        assertThat(t.columnWithName("c4").position()).isEqualTo(4);
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
        assertThat(t.columnNames()).containsExactly("id", "name");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "INT", Types.INTEGER, -1, -1, false, false, false);
        assertColumn(t, "name", "VARCHAR", Types.VARCHAR, 30, -1, false, false, false);
        assertThat(t.columnWithName("id").position()).isEqualTo(1);
        assertThat(t.columnWithName("name").position()).isEqualTo(2);

        t = tables.forTable(new TableId(null, null, "CUSTOMERS_HISTORY"));
        assertThat(t).isNotNull();
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("action", "revision", "changed_on", "id", "name");
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
    public void shouldParseGrantStatement() {
        String ddl = "GRANT ALL PRIVILEGES ON `mysql`.* TO 'mysqluser'@'%'";
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
    public void shouldParseButNotSetUserVariableWithHyphenDelimiter() {
        String ddl = "SET @a-b-c-d:=1";
        parser.parse(ddl, tables);
        assertLocalVariable("a-b-c-d", null);
        assertSessionVariable("a-b-c-d", null);
        assertGlobalVariable("a-b-c-d", null);
    }

    @Test
    public void shouldParseVariableWithHyphenDelimiter() {
        String ddl = "SET a-b-c-d=1";
        parser.parse(ddl, tables);
        assertSessionVariable("a-b-c-d", "1");
    }

    @Test
    public void shouldParseAndIgnoreDeleteStatements() {
        String ddl = "DELETE FROM blah blah";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0);
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    public void shouldParseAndIgnoreInsertStatements() {
        String ddl = "INSERT INTO blah blah";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(0);
        assertThat(listener.total()).isEqualTo(0);
    }

    @Test
    public void shouldParseStatementsWithQuotedIdentifiers() {
        parser.parse(readFile("ddl/mysql-quoted.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(4);
        assertThat(listener.total()).isEqualTo(10);
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
        assertThat(listener.total()).isEqualTo(17);
    }

    @Test
    public void shouldParseCreateStatements() {
        parser.parse(readFile("ddl/mysql-test-create.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(57);
        assertThat(listener.total()).isEqualTo(144);
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

    @Test
    public void shouldParseTestStatements() {
        parser.parse(readFile("ddl/mysql-test-statements.ddl"), tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(6);
        assertThat(listener.total()).isEqualTo(61);
        listener.forEach(this::printEvent);
    }

    @Test
    public void shouldParseSomeLinesFromCreateStatements() {
        parser.parse(readLines(189, "ddl/mysql-test-create.ddl"), tables);
        assertThat(tables.size()).isEqualTo(39);
        assertThat(listener.total()).isEqualTo(120);
    }

    @Test
    public void shouldParseMySql56InitializationStatements() {
        parser.parse(readLines(1, "ddl/mysql-test-init-5.6.ddl"), tables);
        assertThat(tables.size()).isEqualTo(85); // 1 table
        assertThat(listener.total()).isEqualTo(118);
        listener.forEach(this::printEvent);
    }

    @Test
    public void shouldParseMySql57InitializationStatements() {
        parser.parse(readLines(1, "ddl/mysql-test-init-5.7.ddl"), tables);
        assertThat(tables.size()).isEqualTo(123);
        assertThat(listener.total()).isEqualTo(132);
        listener.forEach(this::printEvent);
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

    @FixFor("DBZ-162")
    @Test
    public void shouldParseAlterTableWithNewlineFeeds() {
        String ddl = "CREATE TABLE `test` (id INT(11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "test"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("id");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");
        assertColumn(t, "id", "INT UNSIGNED", Types.INTEGER, 11, -1, false, true, true);

        ddl = "ALTER TABLE `test` CHANGE `id` `collection_id` INT(11)\n UNSIGNED\n NOT NULL\n AUTO_INCREMENT;";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        t = tables.forTable(new TableId(null, null, "test"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("collection_id");
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
        assertThat(t.columnNames()).containsExactly("id", "name", "context", "organization_id", "client_id", "scope_action_ids");
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

    @FixFor("DBZ-198")
    @Test
    public void shouldParseButSkipAlterTableWhenTableIsNotKnown() {
        parser.parse(readFile("ddl/mysql-dbz-198j.ddl"), tables);
//        Testing.Print.enable();
        Testing.print(tables);
        listener.forEach(this::printEvent);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(2);
    }

    @FixFor("DBZ-437")
    @Test
    public void shouldParseStringSameAsKeyword() {
        parser.parse(readFile("ddl/mysql-dbz-437.ddl"), tables);
//        Testing.Print.enable();
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
        assertThat(t.columnNames()).containsExactly("ENCODEDKEY", "ID", "CREATIONDATE", "LASTMODIFIEDDATE", "DATATYPE",
                                                    "ISDEFAULT", "ISREQUIRED", "NAME", "VALUES", "AMOUNTS", "DESCRIPTION",
                                                    "TYPE", "VALUELENGTH", "INDEXINLIST", "CUSTOMFIELDSET_ENCODEDKEY_OID",
                                                    "STATE", "VALIDATIONPATTERN", "VIEWUSAGERIGHTSKEY", "EDITUSAGERIGHTSKEY",
                                                    "BUILTINCUSTOMFIELDID", "UNIQUE");
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
        assertThat(t.columnNames()).containsExactly("a", "b", "c");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "a", "INT", Types.INTEGER, 11, -1, true, false, false);
        assertColumn(t, "b", "INT", Types.INTEGER, -1, -1, true, false, false);
        assertColumn(t, "c", "INT", Types.INTEGER, -1, -1, true, false, false);

        ddl = "ALTER TABLE `s`.`test` CHANGE COLUMN `a` `d` BIGINT(20) NOT NULL AUTO_INCREMENT";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        t = tables.forTable(new TableId(null, "s", "test"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("d", "b", "c");
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
        assertThat(listener.total()).isEqualTo(16);
        listener.forEach(this::printEvent);
    }

    @Test
    public void shouldParseEnumOptions() {
        assertParseEnumAndSetOptions("ENUM('a','b','c')", "a,b,c");
        assertParseEnumAndSetOptions("ENUM('a','multi','multi with () paren', 'other')", "a,multi,multi with () paren,other");
        assertParseEnumAndSetOptions("ENUM('a')", "a");
        assertParseEnumAndSetOptions("ENUM()", "");
        assertParseEnumAndSetOptions("ENUM ('a','b','c') CHARACTER SET", "a,b,c");
        assertParseEnumAndSetOptions("ENUM ('a') CHARACTER SET", "a");
        assertParseEnumAndSetOptions("ENUM () CHARACTER SET", "");
    }

    @Test
    @FixFor("DBZ-476")
    public void shouldParseEscapedEnumOptions() {
        assertParseEnumAndSetOptions("ENUM('a''','b','c')", "a'',b,c");
        assertParseEnumAndSetOptions("ENUM('a\\'','b','c')", "a\\',b,c");
        assertParseEnumAndSetOptions("ENUM(\"a\\\"\",'b','c')", "a\\\",b,c");
        assertParseEnumAndSetOptions("ENUM(\"a\"\"\",'b','c')", "a\"\",b,c");
    }

   @Test
    public void shouldParseSetOptions() {
        assertParseEnumAndSetOptions("SET('a','b','c')", "a,b,c");
        assertParseEnumAndSetOptions("SET('a','multi','multi with () paren', 'other')", "a,multi,multi with () paren,other");
        assertParseEnumAndSetOptions("SET('a')", "a");
        assertParseEnumAndSetOptions("SET()", "");
        assertParseEnumAndSetOptions("SET ('a','b','c') CHARACTER SET", "a,b,c");
        assertParseEnumAndSetOptions("SET ('a') CHARACTER SET", "a");
        assertParseEnumAndSetOptions("SET () CHARACTER SET", "");
    }

    @FixFor("DBZ-160")
    @Test
    public void shouldParseCreateTableWithEnumDefault() {
        String ddl = "CREATE TABLE t ( c1 ENUM('a','b','c') NOT NULL DEFAULT 'b', c2 ENUM('a', 'b', 'c') NOT NULL DEFAULT 'a');";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("c1", "c2");
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
        assertThat(t.columnNames()).containsExactly("c1", "c2");
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
        final String ddl =
                "CREATE TABLE `merge_table` (`id` int(11) NOT NULL, `name` varchar(45) DEFAULT NULL, PRIMARY KEY (`id`)) UNION = (`table1`,`table2`) ENGINE=MRG_MyISAM DEFAULT CHARSET=latin1;";

        parser.parse(ddl, tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);
    }

    @FixFor("DBZ-346")
    @Test
    public void shouldParseAlterTableUnionStatement() {
        final String ddl =
                "CREATE TABLE `merge_table` (`id` int(11) NOT NULL, `name` varchar(45) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=MRG_MyISAM DEFAULT CHARSET=latin1;" +
                "ALTER TABLE `merge_table` UNION = (`table1`,`table2`)";

        parser.parse(ddl, tables);
        Testing.print(tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(2);
    }

    @FixFor("DBZ-419")
    @Test
    public void shouldParseCreateTableWithUnnamedPrimaryKeyConstraint() {
        final String ddl =
                "CREATE TABLE IF NOT EXISTS tables_exception (table_name VARCHAR(100), create_date TIMESTAMP DEFAULT NOW(), enabled INT(1), retention int(1) default 30, CONSTRAINT PRIMARY KEY (table_name));";

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
    @FixFor({"DBZ-408", "DBZ-412"})
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
    @FixFor({"DBZ-408", "DBZ-412", "DBZ-524"})
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
        String ddl = "CREATE TABLE t ( c1 DEC(2) NOT NULL, c2 FIXED(1,0) NOT NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("c1", "c2");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "DEC", Types.DECIMAL, 2, 0, false, false, false);
        assertColumn(t, "c2", "FIXED", Types.DECIMAL, 1, 0, false, false, false);
    }

    @Test
    @FixFor("DBZ-615")
    public void parseDdlForUnscaledDecAndFixed() {
        String ddl = "CREATE TABLE t ( c1 DEC NOT NULL, c2 FIXED(3) NOT NULL);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("c1", "c2");
        assertThat(t.primaryKeyColumnNames()).isEmpty();
        assertColumn(t, "c1", "DEC", Types.DECIMAL, 10, 0, false, false, false);
        assertColumn(t, "c2", "FIXED", Types.DECIMAL, 3, 0, false, false, false);
    }

    @Test
    public void parseTableWithPageChecksum() {
        String ddl =
                "CREATE TABLE t (id INT NOT NULL, PRIMARY KEY (`id`)) PAGE_CHECKSUM=1;" +
                "ALTER TABLE t PAGE_CHECKSUM=0;";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("id");
        assertThat(t.primaryKeyColumnNames()).hasSize(1);
        assertColumn(t, "id", "INT", Types.INTEGER, -1, -1, false, false, false);
    }

    @Test
    @FixFor("DBZ-429")
    public void parseTableWithNegativeDefault() {
        String ddl =
                "CREATE TABLE t (id INT NOT NULL, myvalue INT DEFAULT -10, PRIMARY KEY (`id`));";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table t = tables.forTable(new TableId(null, null, "t"));
        assertThat(t).isNotNull();
        assertThat(t.columnNames()).containsExactly("id", "myvalue");
        assertThat(t.primaryKeyColumnNames()).hasSize(1);
        assertColumn(t, "myvalue", "INT", Types.INTEGER, -1, -1, true, false, false);
    }

    @Test
    @FixFor("DBZ-475")
    public void parseUserDdlStatements() {
        String ddl =
                "CREATE USER 'jeffrey'@'localhost' IDENTIFIED BY 'password';"
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
        String ddl =
                "CREATE TABLE flat_view_request_log (id INT NOT NULL, myvalue INT DEFAULT -10, PRIMARY KEY (`id`));"
              + "ALTER TABLE flat_view_request_log REORGANIZE PARTITION p_max INTO ( PARTITION p_2018_01_17 VALUES LESS THAN ('2018-01-17'), PARTITION p_2018_01_18 VALUES LESS THAN ('2018-01-18'), PARTITION p_max VALUES LESS THAN MAXVALUE);";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-641")
    public void parsePartitionWithEngine() {
        String ddl =
                "CREATE TABLE flat_view_request_log (" +
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

    protected void assertParseEnumAndSetOptions(String typeExpression, String optionString) {
        List<String> options = MySqlDdlParser.parseSetAndEnumOptions(typeExpression);
        String commaSeperatedOptions = Strings.join(",", options);
        assertThat(optionString).isEqualTo(commaSeperatedOptions);
    }

    protected void assertVariable(String name, String expectedValue) {
        String actualValue = parser.systemVariables().getVariable(name);
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        } else {
            assertThat(actualValue).isEqualToIgnoringCase(expectedValue);
        }
    }

    protected void assertVariable(MySqlSystemVariables.Scope scope, String name, String expectedValue) {
        String actualValue = parser.systemVariables().getVariable(name, scope);
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        } else {
            assertThat(actualValue).isEqualToIgnoringCase(expectedValue);
        }
    }

    protected void assertGlobalVariable(String name, String expectedValue) {
        assertVariable(MySqlSystemVariables.Scope.GLOBAL, name, expectedValue);
    }

    protected void assertSessionVariable(String name, String expectedValue) {
        assertVariable(MySqlSystemVariables.Scope.SESSION, name, expectedValue);
    }

    protected void assertLocalVariable(String name, String expectedValue) {
        assertVariable(MySqlSystemVariables.Scope.LOCAL, name, expectedValue);
    }

    protected void printEvent(Event event) {
        Testing.print(event);
    }

    protected String readFile(String classpathResource) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource);) {
            assertThat(stream).isNotNull();
            return IoUtil.read(stream);
        } catch (IOException e) {
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
    protected String readLines(int startingLineNumber, String classpathResource) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(classpathResource);) {
            assertThat(stream).isNotNull();
            StringBuilder sb = new StringBuilder();
            AtomicInteger counter = new AtomicInteger();
            IoUtil.readLines(stream, line -> {
                if (counter.incrementAndGet() >= startingLineNumber) sb.append(line);
                sb.append(System.lineSeparator());
            });
            return sb.toString();
        } catch (IOException e) {
            fail("Unable to read '" + classpathResource + "'");
        }
        assert false : "should never get here";
        return null;
    }

    protected void assertColumn(Table table, String name, String typeName, int jdbcType, int length,
                                String charsetName, boolean optional) {
        Column column = table.columnWithName(name);
        assertThat(column.name()).isEqualTo(name);
        assertThat(column.typeName()).isEqualTo(typeName);
        assertThat(column.jdbcType()).isEqualTo(jdbcType);
        assertThat(column.length()).isEqualTo(length);
        assertThat(column.charsetName()).isEqualTo(charsetName);
        assertThat(column.scale()).isEqualTo(-1);
        assertThat(column.isOptional()).isEqualTo(optional);
        assertThat(column.isGenerated()).isFalse();
        assertThat(column.isAutoIncremented()).isFalse();
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
