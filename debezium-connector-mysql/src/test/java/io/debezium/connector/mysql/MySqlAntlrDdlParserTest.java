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
import io.debezium.doc.FixFor;
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
    @FixFor("debezium/dbz#2401")
    public void shouldParseFlushTablesWithQualifiedTableNames() {
        parser.parse("FLUSH TABLES `mysql`.`user`", tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        parser.parse("FLUSH TABLES mysql.user, other.tbl FOR EXPORT", tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
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
    @FixFor("debezium/dbz#1439")
    public void shouldParseMultipleAddColumnsWithRepeatedInstantAlgorithm() {
        final String ddl = "CREATE TABLE `test_lot` ("
                + "`lot_id` bigint unsigned NOT NULL,"
                + "`trade_date` date NOT NULL,"
                + "PRIMARY KEY (`lot_id`,`trade_date`)"
                + ") ENGINE=InnoDB;"
                + "ALTER TABLE test_lot ADD COLUMN event_ref_type_id INTEGER, algorithm=instant, "
                + "ADD COLUMN event_ref_id BIGINT, algorithm=instant;";

        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        final Table table = tables.forTable(null, null, "test_lot");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(4);

        final Column eventRefTypeId = table.columnWithName("event_ref_type_id");
        final Column eventRefId = table.columnWithName("event_ref_id");

        assertThat(eventRefTypeId).isNotNull();
        assertThat(eventRefTypeId.typeName()).isEqualTo("INTEGER");
        assertThat(eventRefId).isNotNull();
        assertThat(eventRefId.typeName()).isEqualTo("BIGINT");
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

    @Test
    @FixFor("debezium/dbz#2102")
    public void shouldParseEnumOptionsWithCommaQuoteAndBackslash() {
        final String ddl = "CREATE TABLE enum_literals ("
                + "e ENUM('plain','a,b','it''s','back\\\\slash','back\\\\,comma','ends\\\\','')"
                + ");";

        parser.parse(ddl, tables);

        final Table table = tables.forTable(null, null, "enum_literals");
        assertThat(table).isNotNull();
        final Column column = table.columnWithName("e");
        assertThat(column).isNotNull();
        assertThat(MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues()))
                .containsExactly(
                        "plain", "a,b", "it's", "back\\\\slash", "back\\\\,comma", "ends\\\\", "");
    }

    @Test
    @FixFor("debezium/dbz#2102")
    public void shouldParseSetOptionsWithQuoteAndBackslash() {
        final String ddl = "CREATE TABLE set_literals ("
                + "s SET('plain','it''s','back\\\\slash')"
                + ");";

        parser.parse(ddl, tables);

        final Table table = tables.forTable(null, null, "set_literals");
        assertThat(table).isNotNull();
        final Column column = table.columnWithName("s");
        assertThat(column).isNotNull();
        assertThat(MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues()))
                .containsExactly("plain", "it's", "back\\\\slash");
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

    @Test
    @FixFor("debezium/dbz#2291")
    public void shouldParseCreateTableWithAnsiQuotesMode() {
        parser.parse("SET sql_mode='ANSI_QUOTES'", tables);
        String ddl = "CREATE TABLE \"customers\" (\n" +
                "  \"id\" int NOT NULL AUTO_INCREMENT,\n" +
                "  \"first_name\" varchar(255) NOT NULL,\n" +
                "  \"last_name\" varchar(255) NOT NULL,\n" +
                "  \"email\" varchar(255) NOT NULL,\n" +
                "  PRIMARY KEY (\"id\"),\n" +
                "  UNIQUE KEY \"email\" (\"email\")\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "customers");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(4);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("first_name")).isNotNull();
        assertThat(table.columnWithName("last_name")).isNotNull();
        assertThat(table.columnWithName("email")).isNotNull();
        assertThat(table.primaryKeyColumnNames()).containsExactly("id");
    }

    @Test
    @FixFor("debezium/dbz#2291")
    public void shouldParseAlterTableWithAnsiQuotesMode() {
        parser.parse("SET sql_mode='ANSI_QUOTES'", tables);
        parser.parse("CREATE TABLE \"customers\" (\n" +
                "  \"id\" int NOT NULL AUTO_INCREMENT,\n" +
                "  \"first_name\" varchar(255) NOT NULL,\n" +
                "  PRIMARY KEY (\"id\")\n" +
                ") ENGINE=InnoDB", tables);
        parser.parse("ALTER TABLE \"customers\" ADD COLUMN \"middle_name\" varchar(255) NULL", tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "customers");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("middle_name")).isNotNull();
        assertThat(table.columnWithName("middle_name").isOptional()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2291")
    public void shouldParseCreateTableWithAnsiQuotesModeAndFullSqlMode() {
        parser.parse("SET sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE," +
                "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,ANSI_QUOTES'", tables);
        String ddl = "CREATE TABLE \"addresses\" (\n" +
                "  \"id\" int NOT NULL AUTO_INCREMENT,\n" +
                "  \"customer_id\" int NOT NULL,\n" +
                "  \"street\" varchar(255) NOT NULL,\n" +
                "  \"city\" varchar(255) NOT NULL,\n" +
                "  \"state\" varchar(255) NOT NULL,\n" +
                "  \"zip\" varchar(255) NOT NULL,\n" +
                "  \"type\" enum('SHIPPING','BILLING','LIVING') NOT NULL,\n" +
                "  PRIMARY KEY (\"id\"),\n" +
                "  KEY \"address_customer\" (\"customer_id\"),\n" +
                "  CONSTRAINT \"addresses_ibfk_1\" FOREIGN KEY (\"customer_id\") REFERENCES \"customers\" (\"id\")\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "addresses");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(7);
        assertThat(table.columnWithName("type")).isNotNull();
        assertThat(table.columnWithName("type").typeName()).isEqualTo("ENUM");
    }

    @Test
    @FixFor("debezium/dbz#2291")
    public void shouldParseCreateTableWithDefaultModeDoubleQuotedStrings() {
        String ddl = "CREATE TABLE t (\n" +
                "  col1 ENUM(\"a\", \"b\", \"c\"),\n" +
                "  col2 VARCHAR(10) DEFAULT \"test\"\n" +
                ")";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table table = tables.forTable(null, null, "t");
        assertThat(table).isNotNull();
        assertThat(table.columns()).hasSize(2);
    }

    @Test
    @FixFor("debezium/dbz#2291")
    public void shouldParseSchemaQualifiedAlterTableWithAnsiQuotesMode() {
        parser.parse("SET sql_mode='ANSI_QUOTES'", tables);
        parser.parse("CREATE TABLE \"customers\" (\n" +
                "  \"id\" int NOT NULL AUTO_INCREMENT,\n" +
                "  PRIMARY KEY (\"id\")\n" +
                ") ENGINE=InnoDB", tables);
        parser.parse("ALTER TABLE inventory.\"customers\" ADD COLUMN \"middle_name\" varchar(255) NULL", tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2381")
    public void shouldParseNonReservedKeywordsAsUnquotedIdentifiers() {
        String ddl = "CREATE TABLE url (url VARCHAR(700));" +
                "ALTER TABLE url ADD account_id BIGINT;" +
                "ALTER TABLE url CHANGE url url VARCHAR(700) NOT NULL;" +
                "CREATE TABLE auto (auto INT);" +
                "CREATE TABLE manual (manual INT);" +
                "CREATE TABLE offline (offline INT);" +
                "CREATE TABLE online (online INT, id INT);" +
                "CREATE TABLE parallel (parallel INT, id INT);" +
                "CREATE TABLE vector (vector INT);" +
                "CREATE TABLE qualify (qualify INT);" +
                "CREATE TABLE tablesample (tablesample INT);" +
                "ALTER TABLE vector ADD COLUMN online TINYINT;" +
                "ALTER TABLE auto RENAME COLUMN auto TO manual;" +
                "ALTER TABLE parallel ADD INDEX parallel (id);" +
                "ALTER TABLE online ADD CONSTRAINT online CHECK (id > 0);" +
                // COLUMN is optional in the DROP branch of alterListItem.
                "ALTER TABLE vector DROP online;";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();
        assertThat(tables.size()).isEqualTo(9);

        Table url = tables.forTable(null, null, "url");
        assertThat(url.columnWithName("url")).isNotNull();
        assertThat(url.columnWithName("account_id")).isNotNull();

        Table vector = tables.forTable(null, null, "vector");
        assertThat(vector.columnWithName("vector")).isNotNull();
        assertThat(vector.columnWithName("online")).isNull();

        Table auto = tables.forTable(null, null, "auto");
        assertThat(auto.columnWithName("manual")).isNotNull();
        assertThat(auto.columnWithName("auto")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2381")
    public void shouldParseNonReservedKeywordIdentifiersInExtendedDdlClauses() {
        String ddl = "CREATE TABLE IF NOT EXISTS url (id INT PRIMARY KEY);" +
                "CREATE INDEX idx_url ON url (id);" +
                "CREATE TABLE offline (id INT PRIMARY KEY, FOREIGN KEY (id) REFERENCES url (id));" +
                "CREATE TABLE t2 LIKE url;" +
                "ALTER TABLE offline ADD COLUMN online INT;" +
                "ALTER TABLE offline ALTER COLUMN online SET DEFAULT 1;" +
                "ALTER TABLE offline ADD COLUMN c2 INT AFTER online;" +
                "TRUNCATE TABLE t2;" +
                "DROP TABLE IF EXISTS t2;" +
                "CREATE TABLE pt (id INT) PARTITION BY RANGE (id) " +
                "(PARTITION auto VALUES LESS THAN (10), PARTITION pmax VALUES LESS THAN MAXVALUE);" +
                "ALTER TABLE pt DROP PARTITION auto;" +
                "CREATE TABLE typed (url GEOMETRY, manual CHARACTER(5), online SERIAL);" +
                "CREATE TRIGGER parallel BEFORE INSERT ON url FOR EACH ROW SET @x = 1;" +
                "DROP TRIGGER parallel;" +
                "CREATE DATABASE auto;" +
                "USE auto;" +
                "CREATE TABLE t3 (id INT);";
        parser.parse(ddl, tables);
        assertThat(parser.getParsingExceptionsFromWalker()).isEmpty();

        Table offline = tables.forTable(null, null, "offline");
        assertThat(offline.columnWithName("online")).isNotNull();
        assertThat(offline.columnWithName("c2")).isNotNull();

        Table typed = tables.forTable(null, null, "typed");
        assertThat(typed.columnWithName("url")).isNotNull();
        assertThat(typed.columnWithName("manual")).isNotNull();
        assertThat(typed.columnWithName("online")).isNotNull();

        assertThat(tables.forTable(null, null, "t2")).isNull();
        assertThat(tables.forTable("auto", null, "t3")).isNotNull();
    }
}
