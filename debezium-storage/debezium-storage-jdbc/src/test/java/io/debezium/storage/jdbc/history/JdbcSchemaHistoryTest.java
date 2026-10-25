/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
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
        for (String columnName : getColumnsForLargeTable()) {
            sb.append(", ").append(columnName).append(" INT");
        }
        sb.append(");");
        return sb.toString();
    }

    private static List<String> getColumnsForLargeTable() {
        return IntStream.range(0, 400).mapToObj(i -> "thisColumn" + i).collect(Collectors.toList());
    }

    @BeforeAll
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

    @BeforeEach
    public void beforeEach() {
        history = new JdbcSchemaHistory();
        history.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history.start();
    }

    @AfterEach
    public void afterEach() throws IOException {
        if (history != null) {
            history.stop();
        }
        Files.delete(Paths.get(dbFile));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldResolveDialectFromDatabaseProduct() {
        assertTrue(SchemaHistoryDialect.resolve("MySQL").createTable("history").contains("history_data MEDIUMTEXT"));
        assertTrue(SchemaHistoryDialect.resolve("MariaDB").createTable("history").contains("history_data MEDIUMTEXT"));
        assertTrue(SchemaHistoryDialect.resolve("SingleStore").createTable("history").contains("history_data MEDIUMTEXT"));
        assertTrue(SchemaHistoryDialect.resolve("PostgreSQL").createTable("history").contains("history_data TEXT"));
        assertTrue(SchemaHistoryDialect.resolve("CockroachDB").createTable("history").contains("history_data TEXT"));
        assertTrue(SchemaHistoryDialect.resolve("SQLite").createTable("history").contains("history_data TEXT"));
        assertTrue(SchemaHistoryDialect.resolve("Microsoft SQL Server").createTable("history").contains("history_data NVARCHAR(MAX)"));
        assertFalse(SchemaHistoryDialect.resolve("Microsoft SQL Server").createTable("history").contains("history_data VARCHAR(MAX)"));
        assertTrue(SchemaHistoryDialect.resolve("Microsoft SQL Server").createTable("history").contains("record_insert_ts DATETIME2 NOT NULL"));
        assertTrue(SchemaHistoryDialect.resolve("SQL Server").createTable("history").contains("record_insert_ts DATETIME2 NOT NULL"));
        assertTrue(SchemaHistoryDialect.resolve("Azure SQL Edge").createTable("history").contains("history_data NVARCHAR(MAX)"));
        assertTrue(SchemaHistoryDialect.resolve("Azure SQL Edge").createTable("history").contains("record_insert_ts DATETIME2 NOT NULL"));
        assertTrue(SchemaHistoryDialect.resolve("MemSQL").createTable("history").contains("history_data MEDIUMTEXT"));
        assertTrue(SchemaHistoryDialect.resolve("Oracle MySQL").createTable("history").contains("history_data MEDIUMTEXT"));
        assertFalse(SchemaHistoryDialect.resolve("Oracle MySQL").createTable("history").contains("CLOB"));
        assertFalse(SchemaHistoryDialect.resolve("MySQL").createTable("history").contains("DATETIME2"));
        assertTrue(SchemaHistoryDialect.resolve("Oracle").createTable("history").contains("history_data CLOB"));
        assertTrue(SchemaHistoryDialect.resolve("DB2/LINUXX8664").createTable("history").contains("history_data CLOB"));
        assertTrue(SchemaHistoryDialect.resolve("DB2/AIX64").createTable("history").contains("history_data CLOB"));
        assertTrue(SchemaHistoryDialect.resolve("mydb2extra").createTable("history").contains("history_data VARCHAR(65000)"));
        assertFalse(SchemaHistoryDialect.isKnown("mydb2extra"));
        assertFalse(SchemaHistoryDialect.isKnown("Unknown"));
        assertTrue(SchemaHistoryDialect.isKnown("DB2/LINUXX8664"));
        assertTrue(SchemaHistoryDialect.resolve("Unknown").createTable("history").contains("history_data VARCHAR(65000)"));
        assertTrue(SchemaHistoryDialect.resolve((String) null).createTable("history").contains("history_data VARCHAR(65000)"));
        assertTrue(SchemaHistoryDialect.resolve("MySQL").createTable("debezium_database_history").startsWith("CREATE TABLE debezium_database_history"));
        assertTrue(SchemaHistoryDialect.resolve("Oracle").createTable("history").contains("history_data_seq INTEGER NOT NULL"));
        assertEquals("HISTORY", SchemaHistoryDialect.resolve("Oracle").metadataTableName("history"));
        assertEquals("HISTORY", SchemaHistoryDialect.resolve("DB2/LINUXX8664").metadataTableName("history"));
        assertEquals("history", SchemaHistoryDialect.resolve("PostgreSQL").metadataTableName("history"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldResolveDb2ProductNames() {
        for (String product : List.of("DB2", "DB2/LINUXX8664", "DB2 for i", "DB2 for z/OS")) {
            var dialect = SchemaHistoryDialect.resolve(product);
            assertTrue(SchemaHistoryDialect.isKnown(product), product);
            assertTrue(dialect.createTable("history").contains("history_data CLOB"), product);
            assertEquals("SELECT * FROM history FETCH FIRST 1 ROWS ONLY", dialect.tableExistsSelect("history"), product);
        }
        assertFalse(SchemaHistoryDialect.isKnown("mydb2extra"));
        assertFalse(SchemaHistoryDialect.resolve("mydb2extra").createTable("history").contains("CLOB"));
        assertFalse(SchemaHistoryDialect.isKnown("Unknown"));
        assertFalse(SchemaHistoryDialect.resolve("Unknown").createTable("history").contains("CLOB"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldDeclareHistoryDataSeqNotNullForEveryProduct() {
        var products = List.of("MySQL", "MariaDB", "SingleStore", "MemSQL", "PostgreSQL", "CockroachDB", "SQLite",
                "Microsoft SQL Server", "SQL Server", "Azure SQL Edge", "Oracle", "DB2", "DB2/LINUXX8664", "DB2 for i",
                "DB2 for z/OS", "Unknown");
        for (String product : products) {
            assertTrue(SchemaHistoryDialect.resolve(product).createTable("history").contains("history_data_seq INTEGER NOT NULL"), product);
        }
        assertTrue(SchemaHistoryDialect.resolve((String) null).createTable("history").contains("history_data_seq INTEGER NOT NULL"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldResolveExistsSelectFromDatabaseProduct() {
        assertEquals("SELECT TOP 1 * FROM history", SchemaHistoryDialect.resolve("Microsoft SQL Server").tableExistsSelect("history"));
        assertEquals("SELECT TOP 1 * FROM history", SchemaHistoryDialect.resolve("SQL Server").tableExistsSelect("history"));
        assertEquals("SELECT TOP 1 * FROM history", SchemaHistoryDialect.resolve("Azure SQL Edge").tableExistsSelect("history"));
        assertEquals("SELECT * FROM history LIMIT 1", SchemaHistoryDialect.resolve("MySQL").tableExistsSelect("history"));
        assertEquals("SELECT * FROM history LIMIT 1", SchemaHistoryDialect.resolve("PostgreSQL").tableExistsSelect("history"));
        assertEquals("SELECT * FROM history FETCH FIRST 1 ROWS ONLY", SchemaHistoryDialect.resolve("Oracle").tableExistsSelect("history"));
        assertEquals("SELECT * FROM history LIMIT 1", SchemaHistoryDialect.resolve("Oracle MySQL").tableExistsSelect("history"));
        assertEquals("SELECT * FROM history FETCH FIRST 1 ROWS ONLY", SchemaHistoryDialect.resolve("DB2/LINUXX8664").tableExistsSelect("history"));
        assertEquals("SELECT * FROM history LIMIT 1", SchemaHistoryDialect.resolve("Unknown").tableExistsSelect("history"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldKeepExplicitTableDdlOverride() {
        String override = "CREATE TABLE %s (id VARCHAR(36) NOT NULL, history_data TEXT, history_data_seq INTEGER, "
                + "record_insert_ts TIMESTAMP NOT NULL, record_insert_seq INTEGER NOT NULL, PRIMARY KEY (id, history_data_seq))";
        JdbcSchemaHistory schemaHistory = new JdbcSchemaHistory();
        schemaHistory.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_DDL.name(), override)
                .build(), null, SchemaHistoryMetrics.NOOP, true);

        String tableDdl = schemaHistory.getConfig().getTableCreate("MySQL");

        assertTrue(tableDdl.contains("history_data TEXT"));
        assertFalse(tableDdl.contains("MEDIUMTEXT"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldUseDialectWhenTableDdlIsNotSet() {
        JdbcSchemaHistory schemaHistory = (JdbcSchemaHistory) history;
        assertTrue(schemaHistory.getConfig().getTableCreate("MySQL").contains("history_data MEDIUMTEXT"));
        assertTrue(schemaHistory.getConfig().getTableCreate((String) null).contains("history_data VARCHAR(65000)"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldUseFormattedDefaultExistsSelectForNullProductName() {
        JdbcSchemaHistory schemaHistory = (JdbcSchemaHistory) history;
        assertEquals("SELECT * FROM debezium_database_history LIMIT 1", schemaHistory.getConfig().getTableDataExistsSelect((String) null));
        assertEquals("SELECT * FROM debezium_database_history LIMIT 1", schemaHistory.getConfig().getTableDataExistsSelect("Unknown"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldKeepExplicitExistsSelectOverride() {
        String override = "SELECT TOP 1 id FROM %s";
        JdbcSchemaHistory schemaHistory = new JdbcSchemaHistory();
        schemaHistory.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_DATA_EXISTS_SELECT.name(), override)
                .build(), null, SchemaHistoryMetrics.NOOP, true);

        assertEquals("SELECT TOP 1 id FROM debezium_database_history", schemaHistory.getConfig().getTableDataExistsSelect("Microsoft SQL Server"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldResolveMetadataTableNameFromDatabaseProduct() {
        var upperCaseProducts = List.of("Oracle", "DB2", "DB2/LINUXX8664", "DB2 for i", "DB2 for z/OS");
        var unchangedProducts = List.of("PostgreSQL", "CockroachDB", "MySQL", "SQLite", "SQL Server", "Microsoft SQL Server", "Unknown");
        for (String product : upperCaseProducts) {
            assertEquals("DEBEZIUM_DATABASE_HISTORY", SchemaHistoryDialect.resolve(product).metadataTableName("debezium_database_history"), product);
        }
        for (String product : unchangedProducts) {
            assertEquals("debezium_database_history", SchemaHistoryDialect.resolve(product).metadataTableName("debezium_database_history"), product);
        }
        assertEquals("debezium_database_history", SchemaHistoryDialect.resolve((String) null).metadataTableName("debezium_database_history"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldFindTableCreatedByEarlierRelease() throws SQLException, IOException {
        history.stop();
        Files.delete(Paths.get(dbFile));
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
                Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE debezium_database_history (id VARCHAR(36) NOT NULL, history_data VARCHAR(65000), "
                    + "history_data_seq INTEGER, record_insert_ts TIMESTAMP NOT NULL, record_insert_seq INTEGER NOT NULL, "
                    + "PRIMARY KEY (id, history_data_seq))");
        }
        history = new JdbcSchemaHistory();
        history.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history.start();

        assertTrue(history.storageExists());
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldUseFallbackStatementsForUnknownProduct() {
        JdbcSchemaHistory schemaHistory = (JdbcSchemaHistory) history;
        assertTrue(schemaHistory.getConfig().getTableCreate("Unknown").contains("history_data VARCHAR(65000)"));
        assertTrue(schemaHistory.getConfig().getTableCreate("Unknown").contains("history_data_seq INTEGER NOT NULL"));
        assertEquals("SELECT * FROM debezium_database_history LIMIT 1", schemaHistory.getConfig().getTableDataExistsSelect("Unknown"));
        assertEquals("debezium_database_history", schemaHistory.getConfig().getTableMetadataName("Unknown"));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldUseOnlyTablePartForMetadataLookup() {
        history.stop();
        history = new JdbcSchemaHistory();
        history.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_NAME.name(), "public.employee")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history.start();

        JdbcSchemaHistory schemaHistory = (JdbcSchemaHistory) history;
        assertTrue(schemaHistory.storageExists());
        assertEquals("employee", schemaHistory.getConfig().getTableMetadataName("PostgreSQL"));
        assertEquals("EMPLOYEE", schemaHistory.getConfig().getTableMetadataName("Oracle"));
    }

    @Test
    @FixFor("debezium/dbz#70")
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
        assertTrue(schemaHistory.getConfig().getTableCreate("PostgreSQL").startsWith("CREATE TABLE employee"));
        assertTrue(schemaHistory.getConfig().getTableSelect().startsWith("SELECT id, history_data FROM employee"));
        assertTrue(schemaHistory.getConfig().getTableInsert().startsWith("INSERT INTO employee("));
    }

    @Test
    @FixFor("debezium/dbz#70")
    public void shouldNotSplitTableNameWithMoreThanTwoParts() {
        JdbcSchemaHistory schemaHistory = new JdbcSchemaHistory();
        schemaHistory.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_NAME.name(), "a.b.c")
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        assertTrue(schemaHistory.getConfig().getTableCreate("PostgreSQL").startsWith("CREATE TABLE a.b.c"));
    }

    @Test
    public void shouldInitializeStorageWithCredentials() {
        history.initializeStorage();
        history.initializeStorage();
        history.initializeStorage();
        assertTrue(history.storageExists());
        assertFalse(history.exists());
    }

    @Test
    @FixFor("debezium/dbz#1957")
    public void shouldInitializeStorageWithoutCredentials() {
        history.stop();
        history = new JdbcSchemaHistory();
        history.configure(Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .build(), null, SchemaHistoryMetrics.NOOP, true);
        history.start();
        history.initializeStorage();
        assertTrue(history.storageExists());
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

    @Test
    public void shouldNotLeaveTransactionOpenAfterRecovery() throws SQLException, InterruptedException {
        history.record(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);

        Tables tables = new Tables();
        history.recover(source, position, tables, ddlParser);
        assertEquals(tables.size(), 1);

        try (Connection other = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
                Statement stmt = other.createStatement()) {
            // This will fail with SQLITE_BUSY if the recovery tx is still open
            stmt.execute("CREATE TABLE transaction_probe (id INT)");
            stmt.execute("DROP TABLE transaction_probe");
        }
    }

}
