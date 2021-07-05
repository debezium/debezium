/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Collect;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.time.Instant;
import java.util.Map;

import static io.debezium.storage.jdbc.history.JdbcSchemaHistory.JDBC_PASSWORD;
import static io.debezium.storage.jdbc.history.JdbcSchemaHistory.JDBC_URI;
import static io.debezium.storage.jdbc.history.JdbcSchemaHistory.JDBC_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Ismail simsek
 */
public class JdbcSchemaHistoryTest {

    protected SchemaHistory history;
    String dbFile = "/tmp/test.db";
    static String databaseName = "db";
    static String schemaName = "myschema";
    static String ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );";
    static Map<String, Object> source;
    static Map<String, Object> position;
    static TableId tableId;
    static Table table;
    static TableChanges tableChanges;
    static HistoryRecord historyRecord;
    static Map<String, Object> position2;
    static TableId tableId2;
    static Table table2;
    static TableChanges tableChanges2;
    static HistoryRecord historyRecord2;
    static DdlParser ddlParser = new TestAntlrDdlParser();
    static Instant currentInstant = Instant.now();

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
                .with(JDBC_URI, "jdbc:sqlite:" + dbFile)
                .with(JDBC_USER, "user")
                .with(JDBC_PASSWORD, "pass")
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
        history.record(source, position2, databaseName, schemaName, ddl, tableChanges2, currentInstant);
        history.record(source, position2, databaseName, schemaName, ddl, tableChanges2, currentInstant);
        history.stop();
        // after restart, it should recover history correctly
        JdbcSchemaHistory history2 = new JdbcSchemaHistory();
        history2.configure(Configuration.create()
                .with(JDBC_URI, "jdbc:sqlite:" + dbFile)
                .with(JDBC_USER, "user")
                .with(JDBC_PASSWORD, "pass")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history2.start();
        assertTrue(history2.storageExists());
        assertTrue(history2.exists());
        Tables tables2 = new Tables();
        history2.recover(source, position2, tables2, ddlParser);
        assertEquals(tables2.size(), 2);
        assertEquals(tables2.forTable(tableId2), table2);
    }

}
