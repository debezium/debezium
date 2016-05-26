/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlSchemaTest {

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("dbHistory.log");

    private Configurator build;
    private MySqlSchema mysql;
    private SourceInfo source;

    @Before
    public void beforeEach() {
        Testing.Files.delete(TEST_FILE_PATH);
        build = new Configurator();
        mysql = null;
        source = new SourceInfo();
    }

    @After
    public void afterEach() {
        if (mysql != null) {
            try {
                mysql.shutdown();
            } finally {
                mysql = null;
            }
        }
    }

    @Test
    public void shouldApplyDdlStatementsAndRecover() {
        mysql = build.storeDatabaseHistoryInFile(TEST_FILE_PATH).createSchemas();
        mysql.start();

        // Testing.Print.enable();
        source.setBinlogFilename("binlog-001");
        source.setBinlogPosition(400);
        mysql.applyDdl(source, "db1", readFile("ddl/mysql-products.ddl"), this::printStatements);

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded();
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeOnlyFilteredDatabases() {
        mysql = build.storeDatabaseHistoryInFile(TEST_FILE_PATH)
                     .includeDatabases("connector_test")
                     .excludeBuiltInTables()
                     .createSchemas();
        mysql.start();

        source.setBinlogFilename("binlog-001");
        source.setBinlogPosition(400);
        mysql.applyDdl(source, "mysql", readFile("ddl/mysql-test-init-5.7.ddl"), this::printStatements);

        source.setBinlogPosition(1000);
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
    public void shouldLoadSystemAndNonSystemTablesAndConsumeAllDatabases() {
        mysql = build.storeDatabaseHistoryInFile(TEST_FILE_PATH)
                     .includeDatabases("connector_test")
                     .includeBuiltInTables()
                     .createSchemas();
        mysql.start();

        source.setBinlogFilename("binlog-001");
        source.setBinlogPosition(400);
        mysql.applyDdl(source, "mysql", readFile("ddl/mysql-test-init-5.7.ddl"), this::printStatements);

        source.setBinlogPosition(1000);
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

    protected void assertTableIncluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        assertThat(mysql.tables().forTable(tableId)).isNotNull();
        assertThat(mysql.schemaFor(tableId)).isNotNull();
    }

    protected void assertTableExcluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        assertThat(mysql.tables().forTable(tableId)).isNull();
        assertThat(mysql.schemaFor(tableId)).isNull();
    }
    
    protected void assertNoTablesExistForDatabase(String dbName) {
        assertThat(mysql.tables().tableIds().stream().filter(id->id.catalog().equals(dbName)).count()).isEqualTo(0);
    }
    protected void assertTablesExistForDatabase(String dbName) {
        assertThat(mysql.tables().tableIds().stream().filter(id->id.catalog().equals(dbName)).count()).isGreaterThan(0);
    }

    protected void assertHistoryRecorded() {
        MySqlSchema duplicate = build.storeDatabaseHistoryInFile(TEST_FILE_PATH).createSchemas();
        duplicate.loadHistory(source);

        // Make sure table is defined in each ...
        assertThat(duplicate.tables()).isEqualTo(mysql.tables());
        for (int i = 0; i != 2; ++i) {
            duplicate.tables().tableIds().forEach(tableId -> {
                TableSchema dupSchema = duplicate.schemaFor(tableId);
                TableSchema schema = mysql.schemaFor(tableId);
                assertThat(schema).isEqualTo(dupSchema);
                Table dupTable = duplicate.tables().forTable(tableId);
                Table table = mysql.tables().forTable(tableId);
                assertThat(table).isEqualTo(dupTable);
            });
            mysql.tables().tableIds().forEach(tableId -> {
                TableSchema dupSchema = duplicate.schemaFor(tableId);
                TableSchema schema = mysql.schemaFor(tableId);
                assertThat(schema).isEqualTo(dupSchema);
                Table dupTable = duplicate.tables().forTable(tableId);
                Table table = mysql.tables().forTable(tableId);
                assertThat(table).isEqualTo(dupTable);
            });
            duplicate.refreshSchemas();
        }
    }

    protected void printStatements(String dbName, String ddlStatements) {
        Testing.print("Running DDL for '" + dbName + "': " + ddlStatements);
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

}
