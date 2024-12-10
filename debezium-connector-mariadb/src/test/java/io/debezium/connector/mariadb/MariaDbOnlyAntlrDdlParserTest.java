/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

import java.sql.Types;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.connector.mariadb.util.MariaDbValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Testing;

public class MariaDbOnlyAntlrDdlParserTest {

    private MariaDbAntlrDdlParser parser;
    private Tables tables;
    private SimpleDdlParserListener listener;
    private MariaDbValueConverters converters;
    private TableSchemaBuilder tableSchemaBuilder;
    private Properties properties;

    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener) {
        return new MariaDbAntlrDdlParserTest.MariaDbDdlParserWithSimpleTestListener(listener);
    }

    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews) {
        return new MariaDbAntlrDdlParserTest.MariaDbDdlParserWithSimpleTestListener(listener, includeViews);
    }

    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, Tables.TableFilter tableFilter) {
        return new MariaDbAntlrDdlParserTest.MariaDbDdlParserWithSimpleTestListener(listener, tableFilter);
    }

    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews, boolean includeComments) {
        return new MariaDbAntlrDdlParserTest.MariaDbDdlParserWithSimpleTestListener(listener, includeViews, includeComments);
    }

    protected MariaDbValueConverters getValueConverters() {
        return new MariaDbValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.DOUBLE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.PRECISE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN);
    }

    protected MariaDbDefaultValueConverter getDefaultValueConverters(MariaDbValueConverters valueConverters) {
        return new MariaDbDefaultValueConverter(valueConverters);
    }

    public static class MariaDbDdlParserWithSimpleTestListener extends MariaDbAntlrDdlParser {
        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener) {
            this(listener, false);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, Tables.TableFilter tableFilter) {
            this(listener, false, false, tableFilter);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews) {
            this(listener, includeViews, false, Tables.TableFilter.includeAll());
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, boolean includeComments) {
            this(listener, includeViews, includeComments, Tables.TableFilter.includeAll());
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, boolean includeComments, Tables.TableFilter tableFilter) {
            super(false, includeViews, includeComments, tableFilter, new MariaDbCharsetRegistry());
            this.ddlChanges = listener;
        }
    }

    @Before
    public void beforeEach() {
        listener = new SimpleDdlParserListener();
        converters = getValueConverters();
        parser = getParser(listener);
        tables = new Tables();
        tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                getDefaultValueConverters(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(),
                FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false);
        properties = new Properties();
        properties.put("topic.prefix", "test");
    }

    @Test
    @FixFor("DBZ-4661")
    public void shouldSupportCreateTableWithEncryption() {
        parser.parse(
                "CREATE TABLE `t_test_encrypted_test1` " +
                        "(`id` int(11) NOT NULL AUTO_INCREMENT) " +
                        "ENGINE=InnoDB DEFAULT CHARSET=utf8 `ENCRYPTED`=YES COMMENT 'MariaDb encrypted table'",
                tables);
        parser.parse(
                "CREATE TABLE `t_test_encrypted_test2` " +
                        "(`id` int(11) NOT NULL AUTO_INCREMENT) " +
                        "ENGINE=InnoDB DEFAULT CHARSET=utf8 `encrypted`=yes COMMENT 'MariaDb encrypted table'",
                tables);
        parser.parse(
                "CREATE TABLE `t_test_encrypted_test3` " +
                        "(`id` int(11) NOT NULL AUTO_INCREMENT) " +
                        "ENGINE=InnoDB DEFAULT CHARSET=utf8 ENCRYPTED=yes COMMENT 'MariaDb encrypted table'",
                tables);
        parser.parse(
                "CREATE TABLE `t_test_encrypted_test` " +
                        "(`id` int(11) NOT NULL AUTO_INCREMENT) " +
                        "ENGINE=InnoDB DEFAULT CHARSET=utf8 `encrypted`=YES COMMENT 'MariaDb encrypted table'",
                tables);
        parser.parse("CREATE TABLE `t_test_encryption` " +
                "(`id` int(11) NOT NULL AUTO_INCREMENT) " +
                "ENGINE=InnoDB DEFAULT CHARSET=utf8 ENCRYPTION='Y' COMMENT 'Mysql encrypted table';", tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-5201")
    public void shouldSupportMariaDbCurrentTimestamp() {
        String ddl = "CREATE TABLE data(id INT, bi BIGINT(20) NOT NULL DEFAULT unix_timestamp(), PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));
        assertThat(table.columnWithName("id").isOptional()).isFalse();
        assertThat(table.columnWithName("id").hasDefaultValue()).isFalse();

        assertThat(table.columnWithName("bi").isOptional()).isFalse();
        assertThat(table.columnWithName("bi").hasDefaultValue()).isTrue();
        assertThat(getColumnSchema(table, "bi").defaultValue()).isNull();
    }

    @Test
    @FixFor("DBZ-4841")
    public void shouldProcessMariadbCreateIndex() {
        String createIndexDdl = "CREATE INDEX IF NOT EXISTS DX_DT_LAST_UPDATE ON patient(DT_LAST_UPDATE)\n"
                + "WAIT 100\n"
                + "KEY_BLOCK_SIZE=1024M\n"
                + "CLUSTERING =YES\n"
                + "USING RTREE\n"
                + "NOT IGNORED\n"
                + "ALGORITHM = NOCOPY\n"
                + "LOCK EXCLUSIVE";
        parser.parse(createIndexDdl, tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-4675")
    public void shouldSupportCreateTableWithCompressed() {
        parser.parse(
                "CREATE TABLE `my_table_page_compressed1` (\n" +
                        "`column1` bigint(20) NOT NULL,\n" +
                        "`column2` bigint(20) NOT NULL,\n" +
                        "`column3` bigint(20) NOT NULL,\n" +
                        "`column4` bigint(20) NOT NULL,\n" +
                        "`column5` bigint(20) NOT NULL,\n" +
                        "`column6` bigint(20) NOT NULL,\n" +
                        "`column7` bigint(20) NOT NULL,\n" +
                        "`column8` blob,\n" +
                        "`column9` varchar(64) DEFAULT NULL,\n" +
                        "PRIMARY KEY (`column1`),\n" +
                        "KEY `idx_my_index_column2` (`column2`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED `encrypted`=yes `page_compressed`=0",
                tables);
        parser.parse(
                "CREATE TABLE `my_table_page_compressed2` (\n" +
                        "`column1` bigint(20) NOT NULL" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED " +
                        "`encrypted`=yes `page_compressed`=1 `PAGE_COMPRESSION_LEVEL`=0",
                tables);
        parser.parse(
                "CREATE TABLE `my_table_page_compressed3` (\n" +
                        "`column1` bigint(20) NOT NULL" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED " +
                        "`encrypted`=yes page_compressed=1 `page_compression_level`=3",
                tables);
        parser.parse(
                "CREATE TABLE `my_table_page_compressed4` (\n" +
                        "`column1` bigint(20) NOT NULL" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED " +
                        "`encrypted`=yes `page_compressed`=0 PAGE_COMPRESSION_LEVEL=3",
                tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-5424")
    public void shouldProcessNoPrimaryKeyForTable() {
        String ddl = "create table if not exists tbl_without_pk(\n"
                + "id bigint(20) NOT Null,\n"
                + "c1 bigint not null,\n"
                + "c2 int not null,\n"
                + "unique key (c1, (c2*2))\n"
                + ");";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(null, null, "tbl_without_pk");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);

        String createTable = "drop table if exists tbl_without_pk;\n"
                + "create table if not exists tbl_without_pk(\n"
                + "id bigint(20) NOT Null,\n"
                + "c1 bigint not null,\n"
                + "c2 int not null\n"
                + ");\n";
        parser.parse(createTable, tables);
        parser.parse("alter table tbl_without_pk add unique key uk_func(id, (c2*2));", tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        table = tables.forTable(null, null, "tbl_without_pk");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
        parser.parse(createTable, tables);
        parser.parse("create unique index uk_func_2 on tbl_without_pk(id, (c2*2));", tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        table = tables.forTable(null, null, "tbl_without_pk");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-4583")
    public void shouldProcessLargeColumn() {
        String ddl = "create table if not exists tbl_large_col(\n"
                + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "c1 blob(4294967295) NOT NULL,\n"
                + "PRIMARY KEY (`id`)\n"
                + ")";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(null, null, "tbl_large_col");
        assertThat(table.columnWithName("c1").typeName()).isEqualTo("BLOB");
        assertThat(table.columnWithName("c1").length()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    @FixFor({ "DBZ-4497", "DBZ-6185" })
    public void shouldProcessMultipleSignedUnsignedForTable() {
        String ddl = "create table if not exists tbl_signed_unsigned(\n"
                + "`id` bigint(20) ZEROFILL signed UNSIGNED signed ZEROFILL unsigned ZEROFILL NOT NULL AUTO_INCREMENT COMMENT 'ID',\n"
                + "c1 int signed unsigned default '',\n"
                + "c2 decimal(10, 2) SIGNED UNSIGNED ZEROFILL,\n"
                + "c3 float SIGNED ZEROFILL,\n"
                + "c4 double precision(18, 4) UNSIGNED SIGNED ZEROFILL,\n"
                + "c5 smallint zerofill null,\n"
                + "c6 tinyint unsigned zerofill null,\n"
                + "PRIMARY KEY (`id`)\n"
                + ")";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(null, null, "tbl_signed_unsigned");
        assertThat(table.columnWithName("id").typeName()).isEqualTo("BIGINT UNSIGNED ZEROFILL");
        assertThat(table.columnWithName("c1").typeName()).isEqualTo("INT UNSIGNED");
        Column c2 = table.columnWithName("c2");
        assertThat(c2.typeName()).isEqualTo("DECIMAL UNSIGNED ZEROFILL");
        assertThat(c2.length()).isEqualTo(10);
        assertThat(c2.scale().get()).isEqualTo(2);
        assertThat(table.columnWithName("c3").typeName()).isEqualTo("FLOAT UNSIGNED ZEROFILL");
        Column c4 = table.columnWithName("c4");
        assertThat(c4.typeName()).isEqualTo("DOUBLE PRECISION UNSIGNED ZEROFILL");
        assertThat(c4.length()).isEqualTo(18);
        assertThat(c4.scale().get()).isEqualTo(4);
        assertThat(table.columnWithName("c5").typeName()).isEqualTo("SMALLINT UNSIGNED ZEROFILL");
        assertThat(table.columnWithName("c6").typeName()).isEqualTo("TINYINT UNSIGNED ZEROFILL");
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
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(2);
        Table table = tables.forTable("database1", null, "table1");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("x1").charsetName()).isEqualTo("windows-1250");
        table = tables.forTable("database2", null, "table2");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("x1").charsetName()).isEqualTo("latin2");
    }

    @Test
    @FixFor("DBZ-1833")
    public void shouldNotUpdateExistingTable() {
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 INT)";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        parser.parse("CREATE TABLE IF NOT EXISTS mytable (id INT PRIMARY KEY, val1 INT, val2 INT)", tables);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNull();
    }

    @Test
    public void shouldParseCreateUserTable() {
        String ddl = "CREATE TABLE IF NOT EXISTS user (   Host char(60) binary DEFAULT '' NOT NULL, User char(32) binary DEFAULT '' NOT NULL, Select_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Insert_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Update_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Delete_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Drop_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Reload_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Shutdown_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Process_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, File_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Grant_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, References_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Index_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_db_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Super_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tmp_table_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Lock_tables_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Execute_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_slave_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Repl_client_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Show_view_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Alter_routine_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_user_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Event_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Trigger_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, Create_tablespace_priv enum('N','Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, ssl_type enum('','ANY','X509', 'SPECIFIED') COLLATE utf8_general_ci DEFAULT '' NOT NULL, ssl_cipher BLOB NOT NULL, x509_issuer BLOB NOT NULL, x509_subject BLOB NOT NULL, max_questions int(11) unsigned DEFAULT 0  NOT NULL, max_updates int(11) unsigned DEFAULT 0  NOT NULL, max_connections int(11) unsigned DEFAULT 0  NOT NULL, max_user_connections int(11) unsigned DEFAULT 0  NOT NULL, plugin char(64) DEFAULT 'mysql_native_password' NOT NULL, authentication_string TEXT, password_expired ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, password_last_changed timestamp NULL DEFAULT NULL, password_lifetime smallint unsigned NULL DEFAULT NULL, account_locked ENUM('N', 'Y') COLLATE utf8_general_ci DEFAULT 'N' NOT NULL, PRIMARY KEY Host (Host,User) ) engine=MyISAM CHARACTER SET utf8 COLLATE utf8_bin comment='Users and global privileges';";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        Table foo = tables.forTable(new TableId(null, null, "user"));
        assertThat(foo).isNotNull();
        assertThat(foo.retrieveColumnNames()).contains("Host", "User", "Select_priv");
        assertColumn(foo, "Host", "CHAR BINARY", Types.CHAR, 60, -1, false, false, false);
        parser.parse("DROP TABLE user", tables);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-3067")
    public void shouldParseIndex() {
        final String ddl1 = "USE db;"
                + "CREATE TABLE db.t1 (ID INTEGER PRIMARY KEY, val INTEGER, INDEX myidx(val));";
        final String ddl2 = "USE db;"
                + "CREATE OR REPLACE INDEX myidx on db.t1(val);";
        parser = getParser(listener, true);
        parser.parse(ddl1, tables);
        assertThat(tables.size()).isEqualTo(1);
        final Table table = tables.forTable(new TableId(null, "db", "t1"));
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(2);
        parser.parse(ddl2, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(parser.getParsingExceptionsFromWalker().size()).isEqualTo(0);
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

    private void assertColumn(Table table, String name, String typeName, int jdbcType, int length,
                              String charsetName, boolean optional) {
        Column column = table.columnWithName(name);
        Assertions.assertThat(column.name()).isEqualTo(name);
        Assertions.assertThat(column.typeName()).isEqualTo(typeName);
        Assertions.assertThat(column.jdbcType()).isEqualTo(jdbcType);
        Assertions.assertThat(column.length()).isEqualTo(length);
        Assertions.assertThat(column.charsetName()).isEqualTo(charsetName);
        assertFalse(column.scale().isPresent());
        Assertions.assertThat(column.isOptional()).isEqualTo(optional);
        Assertions.assertThat(column.isGenerated()).isFalse();
        Assertions.assertThat(column.isAutoIncremented()).isFalse();
    }

    private void assertColumn(Table table, String name, String typeName, int jdbcType, int length, int scale,
                              boolean optional, boolean generated, boolean autoIncremented) {
        Column column = table.columnWithName(name);
        Assertions.assertThat(column.name()).isEqualTo(name);
        Assertions.assertThat(column.typeName()).isEqualTo(typeName);
        Assertions.assertThat(column.jdbcType()).isEqualTo(jdbcType);
        Assertions.assertThat(column.length()).isEqualTo(length);
        if (scale == Column.UNSET_INT_VALUE) {
            assertFalse(column.scale().isPresent());
        }
        else {
            Assertions.assertThat(column.scale().get()).isEqualTo(scale);
        }
        Assertions.assertThat(column.isOptional()).isEqualTo(optional);
        Assertions.assertThat(column.isGenerated()).isEqualTo(generated);
        Assertions.assertThat(column.isAutoIncremented()).isEqualTo(autoIncremented);
    }

    private void assertColumn(Table table, String name, String typeName, int jdbcType, int length, int scale,
                              boolean optional, boolean generated, boolean autoIncremented,
                              boolean hasDefaultValue, Object defaultValue) {
        Column column = table.columnWithName(name);
        Assertions.assertThat(column.name()).isEqualTo(name);
        Assertions.assertThat(column.typeName()).isEqualTo(typeName);
        Assertions.assertThat(column.jdbcType()).isEqualTo(jdbcType);
        Assertions.assertThat(column.length()).isEqualTo(length);
        if (scale == Column.UNSET_INT_VALUE) {
            assertFalse(column.scale().isPresent());
        }
        else {
            Assertions.assertThat(column.scale().get()).isEqualTo(scale);
        }
        Assertions.assertThat(column.isOptional()).isEqualTo(optional);
        Assertions.assertThat(column.isGenerated()).isEqualTo(generated);
        Assertions.assertThat(column.isAutoIncremented()).isEqualTo(autoIncremented);
        Assertions.assertThat(column.hasDefaultValue()).isEqualTo(hasDefaultValue);
        Assertions.assertThat(getColumnSchema(table, name).defaultValue()).isEqualTo(defaultValue);
    }

    private Schema getColumnSchema(Table table, String column) {
        TableSchema schema = tableSchemaBuilder.create(new DefaultTopicNamingStrategy(properties), table, null, null, null);
        return schema.getEnvelopeSchema().schema().field("after").schema().field(column).schema();
    }
}
