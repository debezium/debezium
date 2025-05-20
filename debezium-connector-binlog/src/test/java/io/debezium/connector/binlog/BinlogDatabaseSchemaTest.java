/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.text.ParsingException;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public abstract class BinlogDatabaseSchemaTest<C extends BinlogConnectorConfig, S extends BinlogDatabaseSchema<P, O, ?, ?>, P extends BinlogPartition, O extends BinlogOffsetContext<?>> {

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("dbHistory.log");
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("testServer", "connector_test", null, null)
            .withDbHistoryPath(TEST_FILE_PATH);

    private static final String SERVER_NAME = "testServer";
    private final String ddlStatements;

    protected S schema;
    protected C connectorConfig;

    protected BinlogDatabaseSchemaTest(String ddlStatements) {
        this.ddlStatements = ddlStatements;
    }

    @Before
    public void beforeEach() {
        DebeziumOpenLineageEmitter.init(Configuration.empty(), "test");
        Testing.Files.delete(TEST_FILE_PATH);
    }

    protected abstract C getConnectorConfig(Configuration config);

    protected abstract S getSchema(Configuration config);

    @After
    public void afterEach() {
        if (schema != null) {
            try {
                schema.close();
            }
            finally {
                schema = null;
            }
        }
    }

    @Test
    public void shouldApplyDdlStatementsAndRecover() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig().build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, "SET " + BinlogSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.parseStreamingDdl(partition, ddlStatements, "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded(config, partition, offset);
    }

    @Test
    public void shouldIgnoreUnparseableDdlAndRecover() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .build();
        schema = getSchema(config);
        schema.initializeStorage();

        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, "SET " + BinlogSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.parseStreamingDdl(partition, "xxxCREATE TABLE mytable\n" + ddlStatements, "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.parseStreamingDdl(partition, ddlStatements, "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertHistoryRecorded(config, partition, offset);
    }

    @Test(expected = ParsingException.class)
    public void shouldFailOnUnparseableDdl() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, "SET " + BinlogSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.parseStreamingDdl(partition, "xxxCREATE TABLE mytable\n" + ddlStatements, "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeOnlyFilteredDatabases() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, "SET " + BinlogSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-test-init-5.7.ddl"), "mysql",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));

        offset.setBinlogStartPoint("binlog.001", 1000);
        schema.parseStreamingDdl(partition, ddlStatements, "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertTableExcluded("mysql.columns_priv");
        assertNoTablesExistForDatabase("mysql");
        assertHistoryRecorded(config, partition, offset);
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeAllDatabases() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .with(BinlogConnectorConfig.TABLE_IGNORE_BUILTIN, false)
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, "SET " + BinlogSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-test-init-5.7.ddl"), "mysql",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));

        offset.setBinlogStartPoint("binlog.001", 1000);
        schema.parseStreamingDdl(partition, ddlStatements, "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        // Check that we have tables ...
        assertTableIncluded("connector_test.products");
        assertTableIncluded("connector_test.products_on_hand");
        assertTableIncluded("connector_test.customers");
        assertTableIncluded("connector_test.orders");
        assertTableIncluded("mysql.columns_priv");
        assertTablesExistForDatabase("mysql");
        assertHistoryRecorded(config, partition, offset);
    }

    @Test
    public void shouldAllowDecimalPrecision() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-decimal-issue.ddl"), "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        assertTableIncluded("connector_test.business_order");
        assertTableIncluded("connector_test.business_order_detail");
        assertHistoryRecorded(config, partition, offset);
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldStoreNonCapturedDatabase() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        schema = getSchema(configFull);
        schema.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableIncluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldNotStoreNonCapturedDatabase() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        schema = getSchema(configFull);
        schema.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldStoreNonCapturedTable() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, "captured.ct")
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        schema = getSchema(configFull);
        schema.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableIncluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldNotStoreNonCapturedTable() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, "captured.ct")
                .build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        schema = getSchema(configFull);
        schema.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");
    }

    @Test
    public void addCommentToSchemaTest() {
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_COMMENTS, true)
                .build();

        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        schema.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> schema.applySchemaChange(x));
        schema.close();

        assertTableSchemaComments("captured.ct", "id", null);
        assertTableSchemaComments("captured.ct", "code", "order code");
    }

    @Test
    @FixFor("DBZ-6945")
    public void shouldProduceCorrectTableChangesForDropStatement() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig().build();
        schema = getSchema(config);
        schema.initializeStorage();
        final P partition = initializePartition(connectorConfig, config);
        final O offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        List<SchemaChangeEvent> schemaChangeEvents = schema.parseStreamingDdl(partition, "DROP TABLE IF EXISTS connector_test.products", "db1",
                offset, Instant.now());

        schema.close();

        assertThat(schemaChangeEvents.size()).isEqualTo(1);
        TableChanges.TableChange tableChange = schemaChangeEvents.get(0).getTableChanges().iterator().next();
        assertThat(tableChange.getTable()).isEqualTo(null);
        assertThat(tableChange.getType()).isEqualTo(TableChanges.TableChangeType.DROP);
        assertThat(tableChange.getId()).isEqualTo(TableId.parse("connector_test.products"));

    }

    protected void assertTableSchemaComments(String tableName, String column, String comments) {
        TableId tableId = TableId.parse(tableName);
        TableSchema tableSchema = schema.schemaFor(tableId);
        Schema valueSchema = tableSchema.valueSchema();
        Field columnField = valueSchema.field(column);
        assertThat(columnField.schema().doc()).isEqualTo(comments);
    }

    protected void assertTableIncluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        TableSchema tableSchema = schema.schemaFor(tableId);
        assertThat(tableSchema).isNotNull();
        assertThat(tableSchema.keySchema().name()).isEqualTo(SchemaNameAdjuster.validFullname(SERVER_NAME + "." + fullyQualifiedTableName + ".Key"));
        assertThat(tableSchema.valueSchema().name()).isEqualTo(SchemaNameAdjuster.validFullname(SERVER_NAME + "." + fullyQualifiedTableName + ".Value"));
    }

    protected void assertTableExcluded(String fullyQualifiedTableName) {
        TableId tableId = TableId.parse(fullyQualifiedTableName);
        assertThat(schema.schemaFor(tableId)).isNull();
    }

    protected void assertNoTablesExistForDatabase(String dbName) {
        assertThat(schema.tableIds().stream().filter(id -> id.catalog().equals(dbName)).count()).isEqualTo(0);
    }

    protected void assertTablesExistForDatabase(String dbName) {
        assertThat(schema.tableIds().stream().filter(id -> id.catalog().equals(dbName)).count()).isGreaterThan(0);
    }

    protected void assertHistoryRecorded(Configuration config, P partition, OffsetContext offset) throws InterruptedException {
        try (S duplicate = getSchema(config)) {
            duplicate.recover(Offsets.of(partition, offset));

            // Make sure table is defined in each ...
            assertThat(duplicate.tableIds()).isEqualTo(schema.tableIds());
            for (int i = 0; i != 2; ++i) {
                duplicate.tableIds().forEach(tableId -> {
                    TableSchema dupSchema = duplicate.schemaFor(tableId);
                    TableSchema tableSchema = schema.schemaFor(tableId);
                    assertThat(tableSchema).isEqualTo(dupSchema);
                    Table dupTable = duplicate.tableFor(tableId);
                    Table table = schema.tableFor(tableId);
                    assertThat(table).isEqualTo(dupTable);
                });
                schema.tableIds().forEach(tableId -> {
                    TableSchema dupSchema = duplicate.schemaFor(tableId);
                    TableSchema tableSchema = schema.schemaFor(tableId);
                    assertThat(tableSchema).isEqualTo(dupSchema);
                    Table dupTable = duplicate.tableFor(tableId);
                    Table table = schema.tableFor(tableId);
                    assertThat(table).isEqualTo(dupTable);
                });
                duplicate.refreshSchemas();
            }
        }
    }

    protected void printStatements(String dbName, Set<TableId> tables, String ddlStatements) {
        Testing.print("Running DDL for '" + dbName + "': " + ddlStatements + " changing tables '" + tables + "'");
    }

    protected abstract P initializePartition(C connectorConfig, Configuration taskConfig);

    protected abstract O initializeOffset(C connectorConfig);

}
