/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import io.debezium.relational.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Collect;

/**
 * @author Ismail simsek
 */
public class JdbcSchemaHistoryTest {

    protected SchemaHistory history;
    String dbFile = "/tmp/test.db";
    static String databaseName = "db";
    static String schemaName = "myschema";
    static String ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );";
    static String ddlLarge = buildDdlLargeTable();
    static Map<String, Object> source;
    static Map<String, Object> position;
    static TableId tableId;
    static Table table;
    static TableChanges tableChanges;
    static HistoryRecord historyRecord;
    static Map<String, Object> positionLarge;
    static TableId tableIdLarge;
    static Table tableLarge;
    static TableChanges tableChangesLarge;
    static HistoryRecord historyRecordLarge;
    static Map<String, Object> position2;
    static TableId tableId2;
    static Table table2;
    static TableChanges tableChanges2;
    static HistoryRecord historyRecord2;
    static DdlParser ddlParser = new TestingAntlrDdlParser();
    static Instant currentInstant = Instant.now();

    private static String buildDdlLargeTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE large (id INT PRIMARY KEY");
        for (String columnName: getColumnsForLargeTable()) {
            sb.append(", ").append(columnName).append(" INT");
        }
        sb.append(");");
        return sb.toString();
    }

    private static List<String> getColumnsForLargeTable() {
        return IntStream.range(0, 400).mapToObj(i -> "thisColumn" + i).collect(Collectors.toList());
    }

    @BeforeClass
    public static void beforeClass() {
        source = Collect.linkMapOf("server", "abc");
        position = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 1);
        tableId = new TableId(databaseName, schemaName, "foo");
        table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor()
                        .name("first")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("first")
                .create();
        tableChanges = new TableChanges().create(table);
        historyRecord = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);
        //
        positionLarge = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 2);
        tableIdLarge = new TableId(databaseName, schemaName, "large");
        TableEditor tableEditorLarge = Table.editor()
                .tableId(tableIdLarge)
                .addColumn(Column.editor()
                        .name("id")
                        .jdbcType(Types.INTEGER)
                        .type("INTEGER")
                        .optional(false)
                        .create());
        getColumnsForLargeTable().forEach(c -> tableEditorLarge.addColumn(Column.editor()
                .name(c)
                .jdbcType(Types.INTEGER)
                .type("INTEGER")
                .optional(false)
                .create()));
        tableLarge = tableEditorLarge
                .setPrimaryKeyNames("id")
                .create();
        tableChangesLarge = new TableChanges().create(tableLarge);
        historyRecordLarge = new HistoryRecord(source, position, databaseName, schemaName, ddlLarge, tableChangesLarge, currentInstant);
        //
        position2 = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 2);
        tableId2 = new TableId(databaseName, schemaName, "bar");
        table2 = Table.editor()
                .tableId(tableId2)
                .addColumn(Column.editor()
                        .name("first")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("first")
                .create();
        tableChanges2 = new TableChanges().create(table2);
        historyRecord2 = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges2, currentInstant);
    }

    @Before
    public void beforeEach() {
        history = new JdbcSchemaHistory();
        history.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history.start();
    }

    @After
    public void afterEach() throws IOException {
        if (history != null) {
            history.stop();
        }
        Files.delete(Paths.get(dbFile));
    }

    @Test
    public void shouldSplitDatabaseAndTableName() {
        JdbcSchemaHistory schemaHistory = new JdbcSchemaHistory();
        schemaHistory.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_NAME.name(), "public.employee")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        assertTrue(schemaHistory.getConfig().getDatabaseName().equalsIgnoreCase("public"));
        assertTrue(schemaHistory.getConfig().getTableName().equalsIgnoreCase("employee"));
    }

    @Test
    public void shouldNotFailMultipleInitializeStorage() {
        history.initializeStorage();
        history.initializeStorage();
        history.initializeStorage();
        assertTrue(history.storageExists());
        assertFalse(history.exists());
    }

    @Test
    public void shouldRecordChangesAndRecover() throws InterruptedException {
        history.record(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);
        history.record(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);
        Tables tables = new Tables();
        history.recover(source, position, tables, ddlParser);
        assertEquals(tables.size(), 1);
        assertEquals(tables.forTable(tableId), table);
        history.record(source, positionLarge, databaseName, schemaName, ddlLarge, tableChangesLarge, currentInstant);
        history.record(source, positionLarge, databaseName, schemaName, ddlLarge, tableChangesLarge, currentInstant);
        history.record(source, position2, databaseName, schemaName, ddl, tableChanges2, currentInstant);
        history.record(source, position2, databaseName, schemaName, ddl, tableChanges2, currentInstant);
        history.stop();
        // after restart, it should recover history correctly
        JdbcSchemaHistory history2 = new JdbcSchemaHistory();
        history2.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history2.start();
        assertTrue(history2.storageExists());
        assertTrue(history2.exists());
        Tables tables2 = new Tables();
        history2.recover(source, position2, tables2, ddlParser);
        assertEquals(tables2.size(), 3);
        assertEquals(tables2.forTable(tableIdLarge), tableLarge);
        assertEquals(tables2.forTable(tableId2), table2);
    }

}
