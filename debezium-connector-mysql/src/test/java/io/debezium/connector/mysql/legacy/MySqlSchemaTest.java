/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.text.ParsingException;
import io.debezium.util.IoUtil;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlSchemaTest {

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("dbHistory.log");
    private static final String SERVER_NAME = "testServer";

    private Configurator build;
    private MySqlSchema mysql;
    private SourceInfo source;

    @Before
    public void beforeEach() {
        Testing.Files.delete(TEST_FILE_PATH);
        build = new Configurator();
        mysql = null;
        source = new SourceInfo(new MySqlConnectorConfig(Configuration.create()
                .with(MySqlConnectorConfig.SERVER_NAME, "server")
                .build()));
    }

    @After
    public void afterEach() {
        if (mysql != null) {
            try {
                mysql.shutdown();
            }
            finally {
                mysql = null;
            }
        }
    }

    @Test
    public void shouldApplyDdlStatementsAndRecover() throws InterruptedException {
        mysql = build.storeDatabaseHistoryInFile(TEST_FILE_PATH).serverName(SERVER_NAME).createSchemas();
        mysql.start();

        // Testing.Print.enable();

        // Set up the server ...
        source.setBinlogStartPoint("binlog-001", 400);
        mysql.applyDdl(source, "db1", "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", this::printStatements);
        mysql.applyDdl(source, "db1", readFile("ddl/mysql-products.ddl"), this::printStatements);

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded();
    }

    @Test
    public void shouldIgnoreUnparseableDdlAndRecover() throws InterruptedException {
        mysql = build
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .storeDatabaseHistoryInFile(TEST_FILE_PATH)
                .serverName(SERVER_NAME)
                .createSchemas();
        mysql.start();

        // Testing.Print.enable();

        // Set up the server ...
        source.setBinlogStartPoint("binlog-001", 400);
        mysql.applyDdl(source, "db1", "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", this::printStatements);
        mysql.applyDdl(source, "db1", "xxxCREATE TABLE mytable\n" + readFile("ddl/mysql-products.ddl"), this::printStatements);
        mysql.applyDdl(source, "db1", readFile("ddl/mysql-products.ddl"), this::printStatements);

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded();
    }

    @Test(expected = ParsingException.class)
    public void shouldFailOnUnparseableDdl() throws InterruptedException {
        mysql = build
                .storeDatabaseHistoryInFile(TEST_FILE_PATH)
                .serverName(SERVER_NAME)
                .createSchemas();
        mysql.start();

        // Testing.Print.enable();

        // Set up the server ...
        source.setBinlogStartPoint("binlog-001", 400);
        mysql.applyDdl(source, "db1", "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", this::printStatements);
        mysql.applyDdl(source, "db1", "xxxCREATE TABLE mytable\n" + readFile("ddl/mysql-products.ddl"), this::printStatements);
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeOnlyFilteredDatabases() throws InterruptedException {
        mysql = build.storeDatabaseHistoryInFile(TEST_FILE_PATH)
                .serverName(SERVER_NAME)
                .includeDatabases("connector_test")
                .excludeBuiltInTables()
                .createSchemas();
        mysql.start();

        source.setBinlogStartPoint("binlog-001", 400);
        mysql.applyDdl(source, "mysql", "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", this::printStatements);
        mysql.applyDdl(source, "mysql", readFile("ddl/mysql-test-init-5.7.ddl"), this::printStatements);

        source.setBinlogStartPoint("binlog-001", 1000);
        mysql.applyDdl(source, "db1", readFile("ddl/mysql-products.ddl"), this::printStatements);

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertTableExcluded("mysql.columns_priv");
        assertNoTablesExistForDatabase("mysql");
        assertHistoryRecorded();
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeAllDatabases() throws InterruptedException {
        mysql = build.storeDatabaseHistoryInFile(TEST_FILE_PATH)
                .serverName(SERVER_NAME)
                .includeDatabases("connector_test,mysql")
                .includeBuiltInTables()
                .createSchemas();
        mysql.start();

        source.setBinlogStartPoint("binlog-001", 400);
        mysql.applyDdl(source, "mysql", "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", this::printStatements);
        mysql.applyDdl(source, "mysql", readFile("ddl/mysql-test-init-5.7.ddl"), this::printStatements);

        source.setBinlogStartPoint("binlog-001", 1000);
        mysql.applyDdl(source, "db1", readFile("ddl/mysql-products.ddl"), this::printStatements);

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertTableIncluded("mysql.columns_priv");
        assertTablesExistForDatabase("mysql");
        assertHistoryRecorded();
    }

    @Test
    public void shouldAllowDecimalPrecision() {
        mysql = build
                .with(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .storeDatabaseHistoryInFile(TEST_FILE_PATH)
                .serverName(SERVER_NAME)
                .createSchemas();
        mysql.start();
        source.setBinlogStartPoint("binlog-001", 400);
        mysql.applyDdl(source, "db1", readFile("ddl/mysql-decimal-issue.ddl"), this::printStatements);

        assertTableIncluded("connector_test.business_order");
        assertTableIncluded("connector_test.business_order_detail");
        assertHistoryRecorded();
    }

    protected void assertTableIncluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        TableSchema tableSchema = mysql.schemaFor(tableId);
        assertThat(tableSchema).isNotNull();
        assertThat(tableSchema.keySchema().name()).isEqualTo(SchemaNameAdjuster.validFullname(SERVER_NAME + "." + fullyQualifiedTableName + ".Key"));
        assertThat(tableSchema.valueSchema().name()).isEqualTo(SchemaNameAdjuster.validFullname(SERVER_NAME + "." + fullyQualifiedTableName + ".Value"));
    }

    protected void assertTableExcluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        assertThat(mysql.schemaFor(tableId)).isNull();
    }

    protected void assertNoTablesExistForDatabase(String dbName) {
        assertThat(mysql.tableIds().stream().filter(id -> id.catalog().equals(dbName)).count()).isEqualTo(0);
    }

    protected void assertTablesExistForDatabase(String dbName) {
        assertThat(mysql.tableIds().stream().filter(id -> id.catalog().equals(dbName)).count()).isGreaterThan(0);
    }

    protected void assertHistoryRecorded() {
        MySqlSchema duplicate = build.storeDatabaseHistoryInFile(TEST_FILE_PATH).createSchemas();
        duplicate.loadHistory(source);

        // Make sure table is defined in each ...
        assertThat(duplicate.tableIds()).isEqualTo(mysql.tableIds());
        for (int i = 0; i != 2; ++i) {
            duplicate.tableIds().forEach(tableId -> {
                TableSchema dupSchema = duplicate.schemaFor(tableId);
                TableSchema schema = mysql.schemaFor(tableId);
                assertThat(schema).isEqualTo(dupSchema);
                Table dupTable = duplicate.tableFor(tableId);
                Table table = mysql.tableFor(tableId);
                assertThat(table).isEqualTo(dupTable);
            });
            mysql.tableIds().forEach(tableId -> {
                TableSchema dupSchema = duplicate.schemaFor(tableId);
                TableSchema schema = mysql.schemaFor(tableId);
                assertThat(schema).isEqualTo(dupSchema);
                Table dupTable = duplicate.tableFor(tableId);
                Table table = mysql.tableFor(tableId);
                assertThat(table).isEqualTo(dupTable);
            });
            duplicate.refreshSchemas();
        }
    }

    protected void printStatements(String dbName, Set<TableId> tables, String ddlStatements) {
        Testing.print("Running DDL for '" + dbName + "': " + ddlStatements + " changing tables '" + tables + "'");
    }

    protected String readFile(String classpathResource) {
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

}
