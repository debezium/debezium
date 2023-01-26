/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener.Event;
import io.debezium.relational.ddl.SimpleDdlParserListener;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Collect;
import io.debezium.util.IoUtil;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Testing;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest {

    private DdlParser parser;
    private Tables tables;
    private SimpleDdlParserListener listener;
    private MySqlValueConverters converters;
    private TableSchemaBuilder tableSchemaBuilder;
    private Properties properties;

    @Before
    public void beforeEach() {
        listener = new SimpleDdlParserListener();
        parser = new MysqlDdlParserWithSimpleTestListener(listener);
        tables = new Tables();
        converters = new MySqlValueConverters(
                JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                BinaryHandlingMode.BYTES);
        tableSchemaBuilder = new TableSchemaBuilder(
                converters,
                new MySqlDefaultValueConverter(converters),
                SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null), SchemaBuilder.struct().build(), false, false);
        properties = new Properties();
        properties.put("topic.prefix", "test");
    }

    @Test
    @FixFor("DBZ-6003")
    public void shouldProcessCreateUniqueBeforePrimaryKeyDefinitions() {
        String ddl = "CREATE TABLE `dbz_6003_1` (\n"
                + "`id` INT ( 11 ) NOT NULL,\n"
                + "`name` VARCHAR ( 255 ),\n"
                + "UNIQUE KEY `test` ( `name` ),\n"
                + "PRIMARY KEY ( `id` )\n"
                + ") ENGINE = INNODB;";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(null, null, "dbz_6003_1");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(1);
        assertThat(table.primaryKeyColumnNames().get(0)).isEqualTo("id");
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // add PRIMARY KEY and UNIQUE KEY
        ddl = "create table dbz_6003_2 (id int not null, name varchar(255));"
                + "alter table dbz_6003_2 add unique key dbz_6003_2_uk(name);"
                + "alter table dbz_6003_2 add primary key(id);";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_2");
        assertThat(table.primaryKeyColumnNames().get(0)).isEqualTo("id");
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // add UNIQUE KEY and PRIMARY KEY
        ddl = "create table dbz_6003_3 (id int not null, name varchar(255));"
                + "alter table dbz_6003_3 add (primary key id(id), unique key dbz_6003_3_uk(name));";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_3");
        assertThat(table.primaryKeyColumnNames().get(0)).isEqualTo("id");
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // add UNIQUE KEY, drop PRIMARY KEY
        ddl = "create table dbz_6003_4 (id int not null, name varchar(255), primary key(id));"
                + "alter table dbz_6003_4 add unique key dbz_6003_4_uk(name);"
                + "alter table dbz_6003_4 drop primary key;";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_4");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // drop PRIMARY KEY, add UNIQUE KEY
        ddl = "create table dbz_6003_5 (id int not null, name varchar(255) not null, primary key(id));"
                + "alter table dbz_6003_5 drop primary key;"
                + "alter table dbz_6003_5 add unique key dbz_6003_5_uk(name);";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_5");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(1);
        assertThat(table.primaryKeyColumnNames().get(0)).isEqualTo("name");
        assertThat(table.columnWithName("name").isOptional()).isFalse();

        // drop UNIQUE KEY, add PRIMARY KEY
        ddl = "create table dbz_6003_6 (id int not null, name varchar(255), unique key dbz_6003_6_uk(name));"
                + "alter table dbz_6003_6 drop index dbz_6003_6_uk;"
                + "alter table dbz_6003_6 add primary key(id);";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_6");
        assertThat(table.primaryKeyColumnNames().get(0)).isEqualTo("id");
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // add PRIMARY KEY, drop UNIQUE KEY
        ddl = "create table dbz_6003_7 (id int not null, name varchar(255), unique key dbz_6003_7_uk(name));"
                + "alter table dbz_6003_7 add primary key(id);"
                + "alter table dbz_6003_7 drop index dbz_6003_7_uk;";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_7");
        assertThat(table.primaryKeyColumnNames().get(0)).isEqualTo("id");
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // drop both PRIMARY and UNIQUE keys
        ddl = "create table dbz_6003_8 (id int not null, name varchar(255), unique key dbz_6003_8_uk(name), primary key(id));"
                + "alter table dbz_6003_8 drop primary key;"
                + "alter table dbz_6003_8 drop index dbz_6003_8_uk;";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_8");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        // create unique index
        ddl = "create table dbz_6003_9 (id int not null, name varchar(255));"
                + "create unique index dbz_6003_9_uk on dbz_6003_9(name);";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_9");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
        assertThat(table.columnWithName("name").isOptional()).isTrue();

        ddl = "create table dbz_6003_10 (id int not null, name varchar(255) not null);"
                + "create unique index dbz_6003_10_uk on dbz_6003_10(name);";
        parser.parse(ddl, tables);
        table = tables.forTable(null, null, "dbz_6003_10");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(1);
        assertThat(table.columnWithName("name").isOptional()).isFalse();
    }

    @Test
    @FixFor("DBZ-5623")
    public void shouldProcessAlterAddDefinitions() {
        String ddl = "create table `some_table` (id bigint(20) not null);"
                + "alter table `some_table` add (primary key `id` (`id`),`k_id` int unsigned not null,`another_field` smallint not null,index `k_id` (`k_id`));"
                + "alter table `some_table` add column (unique key `another_field` (`another_field`));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(null, null, "some_table");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(1);
        assertThat(table.columns().size()).isEqualTo(3);
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
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
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
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        table = tables.forTable(null, null, "tbl_without_pk");
        assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);

        parser.parse(createTable, tables);
        parser.parse("create unique index uk_func_2 on tbl_without_pk(id, (c2*2));", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
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
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "tbl_large_col");

        assertThat(table.columnWithName("c1").typeName()).isEqualTo("BLOB");
        assertThat(table.columnWithName("c1").length()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    @FixFor("DBZ-4497")
    public void shouldProcessMultipleSignedUnsignedForTable() {
        String ddl = "create table if not exists tbl_signed_unsigned(\n"
                + "`id` bigint(20) ZEROFILL signed UNSIGNED signed ZEROFILL unsigned ZEROFILL NOT NULL AUTO_INCREMENT COMMENT 'ID',\n"
                + "c1 int signed unsigned default '',\n"
                + "c2 decimal(10, 2) SIGNED UNSIGNED ZEROFILL,\n"
                + "c3 float SIGNED ZEROFILL,\n"
                + "c4 double precision(18, 4) UNSIGNED SIGNED ZEROFILL,\n"
                + "PRIMARY KEY (`id`)\n"
                + ")";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "tbl_signed_unsigned");

        assertThat(table.columnWithName("id").typeName()).isEqualTo("BIGINT UNSIGNED ZEROFILL");
        assertThat(table.columnWithName("c1").typeName()).isEqualTo("INT UNSIGNED");

        Column c2 = table.columnWithName("c2");
        assertThat(c2.typeName()).isEqualTo("DECIMAL UNSIGNED ZEROFILL");
        assertThat(c2.length()).isEqualTo(10);
        assertThat(c2.scale().get()).isEqualTo(2);

        assertThat(table.columnWithName("c3").typeName()).isEqualTo("FLOAT SIGNED ZEROFILL");

        Column c4 = table.columnWithName("c4");
        assertThat(c4.typeName()).isEqualTo("DOUBLE PRECISION UNSIGNED ZEROFILL");
        assertThat(c4.length()).isEqualTo(18);
        assertThat(c4.scale().get()).isEqualTo(4);
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
    @FixFor("DBZ-4000")
    public void shouldProcessCommentForTable() {
        parser = new MysqlDdlParserWithSimpleTestListener(listener, false, true);
        parser.parse("CREATE TABLE table1(\n"
                + "id INT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'pk',\n"
                + "bin_volume DECIMAL(20, 4) COMMENT 'decimal column'\n"
                + ") COMMENT='add table comment'", tables);
        parser.parse("CREATE TABLE table2(id INT NOT NULL)", tables);
        parser.parse("ALTER TABLE table2 COMMENT='alter table comment'", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(2);

        Table table = tables.forTable(null, null, "table1");
        assertThat(table.columnWithName("id").comment()).isEqualTo("pk");
        assertThat(table.columnWithName("bin_volume").comment()).isEqualTo("decimal column");
        assertThat(table.comment()).isEqualTo("add table comment");
        table = tables.forTable(null, null, "table2");
        assertThat(table.comment()).isEqualTo("alter table comment");
    }

    @Test
    @FixFor("DBZ-4193")
    public void shouldAllowAggregateWindowedFunction() {
        String selectSql = "SELECT e.id,\n"
                + "SUM(e.bin_volume) AS bin_volume,\n"
                + "SUM(e.bin_volume) OVER(PARTITION BY id, e.bin_volume ORDER BY id) AS bin_volume_o,\n"
                + "COALESCE(bin_volume, 0) AS bin_volume2,\n"
                + "COALESCE(LAG(e.bin_volume) OVER(PARTITION BY id ORDER BY e.id), 0) AS bin_volume3,\n"
                + "FIRST_VALUE(id) OVER() AS fv,\n"
                + "DENSE_RANK() OVER(PARTITION BY bin_name ORDER BY id) AS drk,\n"
                + "RANK() OVER(PARTITION BY bin_name) AS rk,\n"
                + "ROW_NUMBER ( ) OVER(PARTITION BY bin_name) AS rn,\n"
                + "NTILE(2) OVER() AS nt\n"
                + "FROM table1 e";
        parser.parse(selectSql, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        selectSql = "SELECT id,\n"
                + "SUM(bin_volume) OVER w AS bin_volume_o,\n"
                + "LAG(bin_volume) OVER w AS bin_volume_l,\n"
                + "LAG(bin_volume, 2) OVER w AS bin_volume_l2,\n"
                + "FIRST_VALUE(id) OVER w2 AS fv,\n"
                + "GROUP_CONCAT(bin_volume order by id) AS `rank`\n"
                + "FROM table1\n"
                + "WINDOW w AS (PARTITION BY id, bin_volume ORDER BY id ROWS UNBOUNDED PRECEDING),\n"
                + "w2 AS (PARTITION BY id, bin_volume ORDER BY id DESC ROWS 10 PRECEDING)";
        parser.parse(selectSql, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor({ "DBZ-4166", "DBZ-4320" })
    public void shouldAllowIndexExpressionForTable() {
        String ddlSql = "CREATE TABLE `cached_sales` (\n"
                + "`id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"
                + "`sale_id` bigint unsigned NOT NULL,\n"
                + "`sale_item_id` bigint unsigned NOT NULL,\n"
                + "`retailer_id` bigint unsigned DEFAULT NULL,\n"
                + "`retailer_branch_id` bigint unsigned DEFAULT NULL,\n"
                + "`retailer_branch_location_id` bigint unsigned NOT NULL,\n"
                + "`sales_area_id` bigint unsigned DEFAULT NULL,\n"
                + "`product_id` bigint unsigned DEFAULT NULL,\n"
                + "`product_variation_id` bigint unsigned NOT NULL,\n"
                + "`season_ids` json DEFAULT NULL,\n"
                + "`category_ids` json DEFAULT NULL,\n"
                + "`color_ids` json DEFAULT NULL,\n"
                + "`size_ids` json DEFAULT NULL,\n"
                + "`gender_ids` json DEFAULT NULL,\n"
                + "`city` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                + "`country_code` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                + "`location_type` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                + "`edi_enabled` tinyint(1) DEFAULT NULL,\n"
                + "`quantity` int DEFAULT NULL,\n"
                + "`brand_normalized_purchase_price_net` int DEFAULT NULL,\n"
                + "`brand_normalized_selling_price_gross` int DEFAULT NULL,\n"
                + "`item_updated_at` datetime DEFAULT NULL,\n"
                + "`sold_at` datetime DEFAULT NULL,\n"
                + "`created_at` timestamp NULL DEFAULT NULL,\n"
                + "`updated_at` timestamp NULL DEFAULT NULL,\n"
                + "`tenant_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n"
                + "PRIMARY KEY (`id`),\n"
                + "UNIQUE KEY `cached_sales_sale_item_id_unique` (`sale_item_id`),\n"
                + "KEY `cached_sales_sale_id_foreign` (`sale_id`),\n"
                + "KEY `cached_sales_retailer_id_foreign` (`retailer_id`),\n"
                + "KEY `cached_sales_retailer_branch_id_foreign` (`retailer_branch_id`),\n"
                + "KEY `cached_sales_retailer_branch_location_id_foreign` (`retailer_branch_location_id`),\n"
                + "KEY `cached_sales_sales_area_id_foreign` (`sales_area_id`),\n"
                + "KEY `cached_sales_product_id_foreign` (`product_id`),\n"
                + "KEY `cached_sales_product_variation_id_foreign` (`product_variation_id`),\n"
                + "KEY `cached_sales_city_index` (`city`),\n"
                + "KEY `cached_sales_country_code_index` (`country_code`),\n"
                + "KEY `cached_sales_location_type_index` (`location_type`),\n"
                + "KEY `cached_sales_sold_at_index` (`sold_at`),\n"
                + "KEY `cached_sales_season_ids_index` ((cast(json_extract(`season_ids`,_utf8mb4'$') as unsigned array))),\n"
                + "KEY `cached_sales_category_ids_index` ((cast(json_extract(`category_ids`,_utf8mb4'$') as unsigned array))),\n"
                + "KEY `cached_sales_color_ids_index` ((cast(json_extract(`color_ids`,_utf8mb4'$') as unsigned array))),\n"
                + "KEY `cached_sales_size_ids_index` ((cast(json_extract(`size_ids`,_utf8mb4'$') as unsigned array))),\n"
                + "KEY `cached_sales_gender_ids_index` (((cast(json_extract(`gender_ids`,_utf8mb4'$') as unsigned array))))\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=13594436 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
        parser.parse(ddlSql, tables);

        ddlSql = "CREATE TABLE `orders_json`(\n"
                + "`id` int NOT NULL AUTO_INCREMENT,\n"
                + "`reward` json DEFAULT NULL,\n"
                + "`additional_info` json DEFAULT NULL,\n"
                + "`created_at` timestamp NULL DEFAULT NULL,\n"
                + "`updated_at` timestamp NULL DEFAULT NULL,\n"
                + "PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB;";
        parser.parse(ddlSql, tables);
        parser.parse("ALTER TABLE orders_json ADD INDEX `idx_order_codes`((cast(json_extract(`additional_info`,_utf8mb4'$.order_codes') as char(17) array)))", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        ddlSql = "CREATE TABLE tbl (\n"
                + "col1 LONGTEXT,\n"
                + "data JSON,\n"
                + "INDEX idx1 ((SUBSTRING(col1, 1, 10))),\n"
                + "INDEX idx2 ((CAST(JSON_EXTRACT(data, _utf8mb4'$') AS UNSIGNED ARRAY))),\n"
                + "INDEX ((CAST(data->>'$.name' AS CHAR(30))))\n"
                + ")";
        parser.parse(ddlSql, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-4229")
    public void shouldProcessQueryWithoutFromStatement() {
        String sp = "CREATE DEFINER=`system_user`@`%` PROCEDURE `update_order`(IN orderID bigint(11))\n" +
                "BEGIN  insert into order_config(order_id, attribute, value, performer)\n" +
                "    SELECT orderID, 'first_attr', 'true', 'AppConfig'\n" +
                "    WHERE NOT EXISTS (select 1 from inventory.order_config t1 where t1.order_id = orderID and t1.attribute = 'first_attr') OR\n" +
                "        EXISTS (select 1 from inventory.order_config p2 where p2.order_id = orderID and p2.attribute = 'first_attr' and p2.performer = 'AppConfig')\n" +
                "    ON DUPLICATE KEY UPDATE value = 'true',\n" +
                "                            performer = 'AppConfig'; -- Enable second_attr for order\n" +
                "END";
        parser.parse(sp, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(0);
    }

    @Test
    public void shouldProcessQueryWithIndexHintPrimary() {
        String sp = "CREATE DEFINER=`my-user`@`%` PROCEDURE `my_table`()\n" +
                "BEGIN\n" +
                "  DECLARE i INT DEFAULT 0;\n" +
                "  DECLARE minID INT DEFAULT 0;\n" +
                "  DECLARE maxID INT DEFAULT 0;\n" +
                "  DECLARE limitSize INT DEFAULT 1000;\n" +
                "  DECLARE total_rows INT DEFAULT 0;\n" +
                "  SET total_rows = (SELECT count(*) FROM db.my_table a);\n" +
                "  WHILE i <= total_rows DO\n" +
                "      SET minID = (select min(a.id) FROM db.my_table a);\n" +
                "      SET maxID = (select max(id) from (select a.id FROM db.my_table a  USE INDEX(PRIMARY) order by a.id asc limit limitSize) top);\n" +
                "      START TRANSACTION;\n" +
                "        DELETE db.my_table FROM db.my_table USE INDEX(PRIMARY) WHERE db.my_table.id >= minId AND db.my_table.id <= maxID ;\n" +
                "      COMMIT;\n" +
                "    do SLEEP(1);\n" +
                "    SET i = i + limitSize;\n" +
                "  END WHILE;\n" +
                "END";
        parser.parse(sp, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(0);
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
        assertThat(getColumnSchema(table, "bin_volume").defaultValue()).isNull();
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
    @FixFor({ "DBZ-2604", "DBZ-4246", "DBZ-4261" })
    public void shouldUseDatabaseCharacterSet() {
        String ddl = "CREATE DATABASE `mydb` character set UTF8mb4 collate utf8mb4_unicode_ci;"
                + "create database `ymsun_test1` charset gb18030 collate gb18030_bi;"
                + "create database `test` charset binary collate binary;"
                + "CREATE TABLE mydb.mytable (id INT PRIMARY KEY, val1 CHAR(16) CHARSET latin2, val2 CHAR(5));"
                + "CREATE TABLE ymsun_test1.mytable (id INT PRIMARY KEY, val1 CHAR(16) CHARSET latin2, val2 CHAR(5));"
                + "CREATE TABLE `test`.`tb1` (`id` int NOT NULL AUTO_INCREMENT,`v1` varchar(255), PRIMARY KEY (`id`));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(3);

        Table table = tables.forTable(null, null, "mydb.mytable");
        assertThat(table.columns()).hasSize(3);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("val2")).isNotNull();
        assertThat(table.columnWithName("val1").charsetName()).isEqualTo("latin2");
        assertThat(table.columnWithName("val2").charsetName()).isEqualTo("UTF8mb4");

        table = tables.forTable(null, null, "ymsun_test1.mytable");
        assertThat(table.columnWithName("val2").charsetName()).isEqualTo("gb18030");

        table = tables.forTable(null, null, "test.tb1");
        assertThat(table.columnWithName("v1").charsetName()).isEqualTo("binary");
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
        String ddl = "CREATE TABLE mytable (id INT PRIMARY KEY, val1 INT, order_Id varchar(128) not null);"
                + "ALTER TABLE mytable ADD COLUMN last_val INT DEFAULT 5;";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        parser.parse("ALTER TABLE mytable ALTER COLUMN last_val DROP DEFAULT;", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns()).hasSize(4);
        assertThat(table.columnWithName("id")).isNotNull();
        assertThat(table.columnWithName("val1")).isNotNull();
        assertThat(table.columnWithName("last_val")).isNotNull();
        assertThat(getColumnSchema(table, "last_val").defaultValue()).isNull();

        parser.parse("ALTER TABLE mytable CHANGE COLUMN last_val last_val INT NOT NULL;", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);

        table = tables.forTable(null, null, "mytable");
        assertThat(table.columnWithName("last_val")).isNotNull();
        assertThat(table.columnWithName("last_val").hasDefaultValue()).isFalse();

        parser.parse("ALTER TABLE mytable CHANGE COLUMN order_Id order_id varchar(255) NOT NULL;", tables);
        table = tables.forTable(null, null, "mytable");
        assertThat(table.columnWithName("order_id")).isNotNull();
        assertThat(table.columnWithName("order_id").length()).isEqualTo(255);

        parser.parse("ALTER TABLE mytable RENAME COLUMN order_id TO order_Id;", tables);
        table = tables.forTable(null, null, "mytable");
        assertThat(table.columnWithName("order_Id")).isNotNull();
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
        assertThat(getColumnSchema(table, "last_val").defaultValue()).isEqualTo(10);
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

        assertThat(tables.forTable(null, null, "mytable1")).isNotNull();
        assertThat(tables.forTable(null, null, "mytable2")).isNotNull();
        assertThat(tables.forTable("db", null, "mytable3")).isNotNull();
        assertThat(tables.forTable("db", null, "mytable4")).isNotNull();
        assertThat(tables.forTable("db", null, "mytable5")).isNotNull();
        assertThat(tables.forTable("db", null, "myta`ble6")).isNotNull();
        assertThat(tables.forTable("db", null, "mytable7`")).isNotNull();
        assertThat(tables.forTable("`db", null, "mytable8")).isNotNull();
        assertThat(tables.forTable("`db", null, "myta\"\"ble9")).isNotNull();
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
        assertThat(table.primaryKeyColumnNames()).isEqualTo(Collections.singletonList("id"));

        parser.parse("ALTER TABLE mytable DROP COLUMN id", tables);
        table = tables.forTable(null, null, "mytable");
        assertThat(table.primaryKeyColumnNames()).isEmpty();
        assertThat(table.primaryKeyColumns()).isEmpty();

        parser.parse("ALTER TABLE mytable ADD PRIMARY KEY(id2)", tables);
        table = tables.forTable(null, null, "mytable");
        assertThat(table.primaryKeyColumnNames()).isEqualTo(Collections.singletonList("id2"));
        assertThat(table.primaryKeyColumns()).hasSize(1);
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
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-4661")
    public void shouldSupportCreateTableWithEcrytion() {
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
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
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
        final String ddl = "CREATE TABLE mytable (id SERIAL, f1 FLOAT, f2 FLOAT(4), f3 FLOAT(7,4), f4 FLOAT(7.4));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        final Table table = tables.forTable(null, null, "mytable");
        assertThat(table.columns().size()).isEqualTo(5);

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

        final Column f4 = table.columnWithName("f4");
        assertThat(f4.typeName()).isEqualTo("FLOAT");
        assertThat(f4.length()).isEqualTo(7);
        assertThat(f4.scale().isPresent()).isFalse();
    }

    @Test
    @FixFor("DBZ-3984")
    public void shouldParseDecimalVariants() {
        String ddl = "CREATE TABLE foo (c1 decimal(19), c2 decimal(19.5), c3 decimal(0.0), c4 decimal(0.2), c5 decimal(19,2));";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        final Table table = tables.forTable(null, null, "foo");

        final Column c1 = table.columnWithName("c1");
        assertThat(c1.typeName()).isEqualTo("DECIMAL");
        assertThat(c1.length()).isEqualTo(19);
        assertThat(c1.scale().get()).isEqualTo(0);

        final Column c2 = table.columnWithName("c2");
        assertThat(c2.typeName()).isEqualTo("DECIMAL");
        assertThat(c2.length()).isEqualTo(19);
        assertThat(c2.scale().get()).isEqualTo(0);

        final Column c3 = table.columnWithName("c3");
        assertThat(c3.typeName()).isEqualTo("DECIMAL");
        assertThat(c3.length()).isEqualTo(10);
        assertThat(c3.scale().get()).isEqualTo(0);

        final Column c4 = table.columnWithName("c4");
        assertThat(c4.typeName()).isEqualTo("DECIMAL");
        assertThat(c4.length()).isEqualTo(10);
        assertThat(c4.scale().get()).isEqualTo(0);

        final Column c5 = table.columnWithName("c5");
        assertThat(c5.typeName()).isEqualTo("DECIMAL");
        assertThat(c5.length()).isEqualTo(19);
        assertThat(c5.scale().get()).isEqualTo(2);
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
    @FixFor({ "DBZ-1150", "DBZ-4174", "DBZ-4640" })
    public void shouldParseCheckTableKeywords() {
        String ddl = "CREATE TABLE my_table (\n" +
                "  user_id varchar(64) NOT NULL,\n" +
                "  upgrade varchar(256),\n" +
                "  quick varchar(256),\n" +
                "  fast varchar(256),\n" +
                "  medium varchar(256),\n" +
                "  extended varchar(256),\n" +
                "  changed varchar(256),\n" +
                "  eur VARCHAR(100),\n" +
                "  iso VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,\n" +
                "  usa VARCHAR(100),\n" +
                "  jis VARCHAR(100),\n" +
                "  internal INT,\n" +
                "  instant BIT,\n" +
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
        assertThat(table.columnWithName("instant")).isNotNull();
    }

    @Test
    @FixFor({ "DBZ-1233", "DBZ-4833" })
    public void shouldParseCheckTableSomeOtherKeyword() {
        List<String> otherKeywords = Collect.arrayListOf("cache", "close", "des_key_file", "end", "export", "flush", "found",
                "general", "handler", "help", "hosts", "install", "mode", "next", "open", "relay", "reset", "slow",
                "soname", "traditional", "triggers", "uninstall", "until", "use_frm", "user_resources", "lag", "lead",
                "first_value", "last_value", "cume_dist", "dense_rank", "percent_rank", "rank", "row_number",
                "nth_value", "ntile");

        String columnDefs = otherKeywords.stream().map(m -> m + " varchar(256)").collect(Collectors.joining(", "));
        String ddl = "create table t_keywords(" + columnDefs + ");";

        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        final Table table = tables.forTable(null, null, "t_keywords");
        assertThat(table).isNotNull();
        otherKeywords.stream().forEach(f -> assertThat(table.columnWithName(f)).isNotNull());
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
    @FixFor("DBZ-3969")
    public void shouldParseNonBinaryStringWithBinaryCollationAsString() {
        String ddl = "CREATE TABLE non_binary ( "
                + "c1 CHAR(60) BINARY PRIMARY KEY,"
                + "c2 VARCHAR(60) BINARY NOT NULL DEFAULT '',"
                + "c3 TINYTEXT BINARY NOT NULL DEFAULT 'N',"
                + "c4 TEXT BINARY,"
                + "c5 MEDIUMTEXT BINARY,"
                + "c6 LONGTEXT BINARY,"
                + "c7 NCHAR(60) BINARY,"
                + "c8 NVARCHAR(60) BINARY"
                + ") engine=MyISAM CHARACTER SET utf8 COLLATE utf8_bin";
        parser.parse(ddl, tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        assertThat(tables.size()).isEqualTo(1);
        Table table = tables.forTable(null, null, "non_binary");
        assertThat(table.columns()).hasSize(8);
        assertColumn(table, "c1", "CHAR BINARY", Types.CHAR, 60, -1, false, false, false, false, null);
        assertColumn(table, "c2", "VARCHAR BINARY", Types.VARCHAR, 60, -1, false, false, false, true, "");
        assertColumn(table, "c3", "TINYTEXT BINARY", Types.VARCHAR, -1, -1, false, false, false, true, "N");
        assertColumn(table, "c4", "TEXT BINARY", Types.VARCHAR, -1, -1, true, false, false, true, null);
        assertColumn(table, "c5", "MEDIUMTEXT BINARY", Types.VARCHAR, -1, -1, true, false, false, true, null);
        assertColumn(table, "c6", "LONGTEXT BINARY", Types.VARCHAR, -1, -1, true, false, false, true, null);
        assertColumn(table, "c7", "NCHAR BINARY", Types.NCHAR, 60, -1, true, false, false, true, null);
        assertColumn(table, "c8", "NVARCHAR BINARY", Types.NVARCHAR, 60, -1, true, false, false, true, null);
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

        ddl = "ALTER TABLE t ADD COLUMN col4 INT(10) DEFAULT ' 29 ' COLLATE 'utf8_general_ci';";
        parser.parse(ddl, tables);
        Table t4 = tables.forTable(new TableId(null, null, "t"));
        assertThat(t4).isNotNull();
        assertThat(t4.retrieveColumnNames()).containsExactly("col1", "col3", "col2", "col4");
        assertThat(t4.primaryKeyColumnNames()).isEmpty();
        assertColumn(t4, "col1", "VARCHAR", Types.VARCHAR, 25, -1, true, false, false);
        assertColumn(t4, "col3", "FLOAT", Types.FLOAT, -1, -1, false, false, false);
        assertColumn(t4, "col2", "VARCHAR", Types.VARCHAR, 50, -1, false, false, false);
        assertColumn(t4, "col4", "INT", Types.INTEGER, 10, -1, true, false, false);
        assertThat(t4.columnWithName("col1").position()).isEqualTo(1);
        assertThat(t4.columnWithName("col3").position()).isEqualTo(2);
        assertThat(t4.columnWithName("col2").position()).isEqualTo(3);
        assertThat(t4.columnWithName("col4").position()).isEqualTo(4);
        assertThat(t4.columnWithName("col4").defaultValueExpression().get()).isEqualTo(" 29 ");
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
    @FixFor({ "DBZ-169", "DBZ-4503" })
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
                + "ADD PRIMARY KEY (ID, revision);";
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

        parser.parse("ALTER TABLE `CUSTOMERS_HISTORY` DROP COLUMN ID", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        t = tables.forTable(new TableId(null, null, "CUSTOMERS_HISTORY"));
        assertThat(t.primaryKeyColumnNames().size()).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-4786")
    public void shouldParseCreateAndRemoveTwiceOrDoesNotExist() {
        String ddl = "CREATE TABLE customers ( "
                + "id INT PRIMARY KEY NOT NULL, "
                + "name VARCHAR(30) NOT NULL, "
                + "PRIMARY KEY (id) );";
        parser.parse(ddl, tables);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(listener.total()).isEqualTo(1);

        Table t = tables.forTable(new TableId(null, null, "customers"));
        assertThat(t).isNotNull();
        assertThat(t.retrieveColumnNames()).containsExactly("id", "name");
        assertThat(t.primaryKeyColumnNames()).containsExactly("id");

        parser.parse("ALTER TABLE customers DROP COLUMN name", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
        t = tables.forTable(new TableId(null, null, "customers"));
        assertThat(t.columnWithName("NAME")).isEqualTo(null);

        parser.parse("ALTER TABLE customers DROP COLUMN name", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);

        parser.parse("ALTER TABLE customers DROP COLUMN not_exists", tables);
        assertThat(((MySqlAntlrDdlParser) parser).getParsingExceptionsFromWalker().size()).isEqualTo(0);
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
    @FixFor("DBZ-5836")
    public void parseCreateUserDdlStatement() {
        String ddl = "CREATE USER 'test_crm_debezium'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9' PASSWORD EXPIRE NEVER COMMENT '-';";
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
        assertThat(getColumnSchema(table, "columnA").defaultValue()).isEqualTo("A");
        assertThat(getColumnSchema(table, "columnB").defaultValue()).isEqualTo(1);
        assertThat(getColumnSchema(table, "columnC").defaultValue()).isEqualTo("C");
        assertThat(getColumnSchema(table, "columnD").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "columnE").defaultValue()).isEqualTo(null);
        assertThat(getColumnSchema(table, "my_dateA").defaultValue()).isEqualTo(LocalDateTime.of(2018, 4, 27, 13, 28, 43).toEpochSecond(ZoneOffset.UTC) * 1_000);
        assertThat(getColumnSchema(table, "my_dateB").defaultValue()).isEqualTo(LocalDateTime.of(9999, 12, 31, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000);
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
        assertThat(getColumnSchema(tableDef, "id").defaultValue()).isEqualTo(0);

        ddl = "DROP TABLE IF EXISTS data; " +
                "CREATE TABLE data(id INT DEFAULT 1, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        table = tables.forTable(new TableId(null, null, "data"));
        assertThat(table.columnWithName("id").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("id").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "id").defaultValue()).isEqualTo(1);
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
        String isoEpoch = ZonedTimestamp.toIsoString(zdt, ZoneOffset.UTC, MySqlValueConverters::adjustTemporal, null);

        assertThat(table.columnWithName("ts_col").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col").defaultValue()).isEqualTo(isoEpoch);

        assertThat(table.columnWithName("ts_col2").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col2").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col2").defaultValue()).isEqualTo(isoEpoch);

        assertThat(table.columnWithName("ts_col3").isOptional()).isEqualTo(true);
        assertThat(table.columnWithName("ts_col3").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col3").defaultValue()).isNull();

        final String alter1 = "ALTER TABLE my_table " +
                " ADD ts_col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;";

        parser.parse(alter1, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columns().size()).isEqualTo(4);
        assertThat(table.columnWithName("ts_col4").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col4").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col4").defaultValue()).isEqualTo(isoEpoch);

        final String alter2 = "ALTER TABLE my_table " +
                " ADD ts_col5 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP";

        parser.parse(alter2, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columns().size()).isEqualTo(5);
        assertThat(table.columnWithName("ts_col5").isOptional()).isEqualTo(false);
        assertThat(table.columnWithName("ts_col5").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col5").defaultValue()).isEqualTo(isoEpoch);
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
        assertThat(getColumnSchema(table, "ts_col01").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col02").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col02").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col03").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col03").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col04").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col04").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));
        assertThat(table.columnWithName("ts_col05").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col05").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:00:00"));
        assertThat(table.columnWithName("ts_col06").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col06").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:00:00"));
        assertThat(table.columnWithName("ts_col07").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col07").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:00:00"));
        assertThat(table.columnWithName("ts_col08").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col08").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col09").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col09").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col10").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col10").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col11").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col11").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05.123456"));
        assertThat(table.columnWithName("ts_col12").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col12").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col13").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col13").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col14").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col14").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col15").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col15").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col16").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col16").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col17").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col17").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col18").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col18").defaultValue()).isEqualTo(toIsoString("1970-01-01 00:00:01"));
        assertThat(table.columnWithName("ts_col19").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col19").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
        assertThat(table.columnWithName("ts_col20").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col20").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col21").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col21").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col22").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col22").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col23").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col23").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col24").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col24").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));
        assertThat(table.columnWithName("ts_col25").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col25").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:00"));

        final String alter1 = "ALTER TABLE my_table ADD ts_col TIMESTAMP DEFAULT '2020-01-02';";

        parser.parse(alter1, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columnWithName("ts_col").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col").defaultValue()).isEqualTo(toIsoString("2020-01-02 00:00:00"));

        final String alter2 = "ALTER TABLE my_table MODIFY ts_col TIMESTAMP DEFAULT '2020-01-02:03:04:05';";

        parser.parse(alter2, tables);
        table = tables.forTable(new TableId(null, null, "my_table"));

        assertThat(table.columnWithName("ts_col").hasDefaultValue()).isEqualTo(true);
        assertThat(getColumnSchema(table, "ts_col").defaultValue()).isEqualTo(toIsoString("2020-01-02 03:04:05"));
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

    private String toIsoString(String timestamp) {
        return ZonedTimestamp.toIsoString(Timestamp.valueOf(timestamp).toInstant().atZone(ZoneId.systemDefault()), null, null);
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

    private void assertColumn(Table table, String name, String typeName, int jdbcType, int length, int scale,
                              boolean optional, boolean generated, boolean autoIncremented,
                              boolean hasDefaultValue, Object defaultValue) {
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
        assertThat(column.hasDefaultValue()).isEqualTo(hasDefaultValue);
        assertThat(getColumnSchema(table, name).defaultValue()).isEqualTo(defaultValue);
    }

    class MysqlDdlParserWithSimpleTestListener extends MySqlAntlrDdlParser {

        MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener) {
            this(changesListener, false);
        }

        MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, TableFilter tableFilter) {
            this(changesListener, false, false, tableFilter);
        }

        MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews) {
            this(changesListener, includeViews, false, TableFilter.includeAll());
        }

        MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments) {
            this(changesListener, includeViews, includeComments, TableFilter.includeAll());
        }

        private MysqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, TableFilter tableFilter) {
            super(false,
                    includeViews,
                    includeComments,
                    converters,
                    tableFilter);
            this.ddlChanges = changesListener;
        }
    }

    private Schema getColumnSchema(Table table, String column) {
        TableSchema schema = tableSchemaBuilder.create(new DefaultTopicNamingStrategy(properties), table, null, null, null);
        return schema.getEnvelopeSchema().schema().field("after").schema().field(column).schema();
    }
}
