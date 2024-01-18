/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

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

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.text.ParsingException;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlDatabaseSchemaTest {

    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("dbHistory.log");
    private final UniqueDatabase DATABASE = new UniqueDatabase("testServer", "connector_test", null, null)
            .withDbHistoryPath(TEST_FILE_PATH);

    private static final String SERVER_NAME = "testServer";

    private MySqlDatabaseSchema mysql;
    private MySqlConnectorConfig connectorConfig;

    @Before
    public void beforeEach() {
        Testing.Files.delete(TEST_FILE_PATH);
    }

    private MySqlDatabaseSchema getSchema(Configuration config) {
        config = config.edit().with(AbstractSchemaHistory.INTERNAL_PREFER_DDL, true).build();
        connectorConfig = new MySqlConnectorConfig(config);
        final MySqlValueConverters mySqlValueConverters = new MySqlValueConverters(
                DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE,
                BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                MySqlValueConverters::adjustTemporal,
                MySqlValueConverters::defaultParsingErrorHandler,
                connectorConfig.getConnectorAdapter());
        return new MySqlDatabaseSchema(
                connectorConfig,
                mySqlValueConverters,
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.create(),
                false);
    }

    @After
    public void afterEach() {
        if (mysql != null) {
            try {
                mysql.close();
            }
            finally {
                mysql = null;
            }
        }
    }

    @Test
    public void shouldApplyDdlStatementsAndRecover() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig().build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

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
        mysql = getSchema(config);
        mysql.initializeStorage();

        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(partition, "xxxCREATE TABLE mytable\n" + IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

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
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(partition, "xxxCREATE TABLE mytable\n" + IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
    }

    @Test
    public void shouldLoadSystemAndNonSystemTablesAndConsumeOnlyFilteredDatabases() throws InterruptedException {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, true)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-test-init-5.7.ddl"), "mysql",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        offset.setBinlogStartPoint("binlog.001", 1000);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

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
                .with(MySqlConnectorConfig.TABLE_IGNORE_BUILTIN, false)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, "SET " + MySqlSystemVariables.CHARSET_NAME_SERVER + "=utf8mb4", null,
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-test-init-5.7.ddl"), "mysql",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));

        offset.setBinlogStartPoint("binlog.001", 1000);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-products.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

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
    public void shouldAllowDecimalPrecision() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-decimal-issue.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

        assertTableIncluded("connector_test.business_order");
        assertTableIncluded("connector_test.business_order_detail");
        assertHistoryRecorded(config, partition, offset);
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldStoreNonCapturedDatabase() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableIncluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldNotStoreNonCapturedDatabase() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableExcluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldStoreNonCapturedTable() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "captured.ct")
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableIncluded("captured.nct");
        assertTableIncluded("non_captured.nct");
    }

    @Test
    @FixFor("DBZ-3622")
    public void shouldNotStoreNonCapturedTable() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfigWithoutDatabaseFilter()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "captured.ct")
                .build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");

        final Configuration configFull = DATABASE.defaultConfigWithoutDatabaseFilter().build();
        mysql = getSchema(configFull);
        mysql.recover(Offsets.of(partition, offset));

        assertTableIncluded("captured.ct");
        assertTableExcluded("captured.nct");
        assertTableExcluded("non_captured.nct");
    }

    @Test
    public void addCommentToSchemaTest() {
        final Configuration config = DATABASE.defaultConfig()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "captured")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_COMMENTS, true)
                .build();

        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        mysql.parseStreamingDdl(partition, IoUtil.readClassPathResource("ddl/mysql-schema-captured.ddl"), "db1",
                offset, Instant.now()).forEach(x -> mysql.applySchemaChange(x));
        mysql.close();

        assertTableSchemaComments("captured.ct", "id", null);
        assertTableSchemaComments("captured.ct", "code", "order code");
    }

    @Test
    @FixFor("DBZ-6945")
    public void shouldProduceCorrectTableChangesForDropStatement() {
        // Testing.Print.enable();
        final Configuration config = DATABASE.defaultConfig().build();
        mysql = getSchema(config);
        mysql.initializeStorage();
        final MySqlPartition partition = initializePartition(connectorConfig, config);
        final MySqlOffsetContext offset = initializeOffset(connectorConfig);

        // Set up the server ...
        offset.setBinlogStartPoint("binlog.001", 400);
        List<SchemaChangeEvent> schemaChangeEvents = mysql.parseStreamingDdl(partition, "DROP TABLE IF EXISTS connector_test.products", "db1",
                offset, Instant.now());

        mysql.close();

        assertThat(schemaChangeEvents.size()).isEqualTo(1);
        TableChanges.TableChange tableChange = schemaChangeEvents.get(0).getTableChanges().iterator().next();
        assertThat(tableChange.getTable()).isEqualTo(null);
        assertThat(tableChange.getType()).isEqualTo(TableChanges.TableChangeType.DROP);
        assertThat(tableChange.getId()).isEqualTo(TableId.parse("connector_test.products"));

    }

    protected void assertTableSchemaComments(String tableName, String column, String comments) {
        TableId tableId = TableId.parse(tableName);
        TableSchema tableSchema = mysql.schemaFor(tableId);
        Schema valueSchema = tableSchema.valueSchema();
        Field columnField = valueSchema.field(column);
        assertThat(columnField.schema().doc()).isEqualTo(comments);
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

    protected void assertHistoryRecorded(Configuration config, MySqlPartition partition, OffsetContext offset) {
        try (MySqlDatabaseSchema duplicate = getSchema(config)) {
            duplicate.recover(Offsets.of(partition, offset));

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
    }

    protected void printStatements(String dbName, Set<TableId> tables, String ddlStatements) {
        Testing.print("Running DDL for '" + dbName + "': " + ddlStatements + " changing tables '" + tables + "'");
    }

    private MySqlPartition initializePartition(MySqlConnectorConfig connectorConfig, Configuration taskConfig) {
        Set<MySqlPartition> partitions = (new MySqlPartition.Provider(connectorConfig, taskConfig)).getPartitions();
        assertThat(partitions.size()).isEqualTo(1);

        return partitions.iterator().next();
    }

    private MySqlOffsetContext initializeOffset(MySqlConnectorConfig connectorConfig) {
        return MySqlOffsetContext.initial(connectorConfig);
    }
}
