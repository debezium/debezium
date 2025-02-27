/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

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
