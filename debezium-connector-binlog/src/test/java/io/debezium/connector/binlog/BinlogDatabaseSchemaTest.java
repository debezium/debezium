/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.Types;
import java.time.Instant;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.EnumeratedValues;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Column;
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
    private static final TableId METADATA_KEY_TABLE_ID = new TableId("connector_test", null, "metadataKeys");
    private final String ddlStatements;

    protected S schema;
    protected C connectorConfig;

    protected BinlogDatabaseSchemaTest(String ddlStatements) {
        this.ddlStatements = ddlStatements;
    }

    @BeforeEach
    void beforeEach() {
        Testing.Files.delete(TEST_FILE_PATH);
    }

    protected abstract C getConnectorConfig(Configuration config);

    protected abstract S getSchema(Configuration config);

    @Test
    void shouldRejectMissingBinlogMetadataKey() {
        schema = getSchema(metadataKeyConfig().build());
        assertThatThrownBy(() -> schema.validateBinlogMetadataKey(metadataKeyTable()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(METADATA_KEY_TABLE_ID.toString())
                .hasMessageContaining("message.key.columns");
    }

    @Test
    void shouldRejectNullableUniqueBinlogMetadataKey() {
        schema = getSchema(metadataKeyConfig().build());
        final Table table = metadataKeyTable().edit().setPrimaryKeyNames("candidate").create();
        assertThatThrownBy(() -> schema.validateBinlogMetadataKey(table))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Nullable unique indexes");
    }

    @Test
    void shouldRejectUnmatchedBinlogMetadataKeyConfiguration() {
        schema = getSchema(metadataKeyConfig()
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "other.table:id;connector_test.metadataKeys:missing")
                .build());
        final Table table = metadataKeyTable().edit().setPrimaryKeyNames("candidate").create();
        assertThatThrownBy(() -> schema.validateBinlogMetadataKey(table))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    void shouldAllowExplicitBinlogMetadataKey() {
        schema = getSchema(metadataKeyConfig()
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "connector_test.metadataKeys:candidate")
                .build());
        assertThatCode(() -> schema.validateBinlogMetadataKey(metadataKeyTable())).doesNotThrowAnyException();
    }

    @Test
    void shouldAllowNonNullableBinlogMetadataKey() {
        schema = getSchema(metadataKeyConfig().build());
        final Table table = metadataKeyTable().edit().setPrimaryKeyNames("id").create();
        assertThatCode(() -> schema.validateBinlogMetadataKey(table)).doesNotThrowAnyException();
    }

    @Test
    void shouldAllowMissingKeyWhenBinlogMetadataModeIsDisabled() {
        schema = getSchema(metadataKeyConfig().with(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA, false).build());
        assertThatCode(() -> schema.validateBinlogMetadataKey(metadataKeyTable())).doesNotThrowAnyException();
    }

    @Test
    void shouldIgnoreFilteredTablesInBinlogMetadataKeyValidation() {
        schema = getSchema(metadataKeyConfig().build());
        final Table table = metadataKeyTable().edit().tableId(new TableId("connector_test", null, "excluded")).create();
        assertThatCode(() -> schema.validateBinlogMetadataKey(table)).doesNotThrowAnyException();
        final TableMapEventData metadata = metadataKeyTableMap();
        metadata.setTable("excluded");
        metadata.getEventMetadata().setSimplePrimaryKeys(List.of());
        assertThat(schema.registerTableFromBinlogMetadata(metadata)).isFalse();
    }

    @Test
    void shouldRejectBinlogMetadataKeyAfterPrimaryKeyDropped() {
        schema = getSchema(metadataKeyConfig().build());
        final TableMapEventData metadata = metadataKeyTableMap();
        assertThat(schema.registerTableFromBinlogMetadata(metadata)).isTrue();
        metadata.getEventMetadata().setSimplePrimaryKeys(List.of());
        assertThatThrownBy(() -> schema.registerTableFromBinlogMetadata(metadata))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    void shouldRejectInconsistentBinlogMetadataKey() {
        schema = getSchema(metadataKeyConfig().build());
        schema.refresh(metadataKeyTable().edit().setPrimaryKeyNames("candidate").create());
        assertThatThrownBy(() -> schema.registerTableFromBinlogMetadata(metadataKeyTableMap()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("differ from the snapshot or DDL key columns");
    }

    @Test
    void shouldPreserveExplicitBinlogMetadataKeyAcrossRebuild() {
        schema = getSchema(metadataKeyConfig()
                .with(BinlogConnectorConfig.MSG_KEY_COLUMNS, "connector_test.metadataKeys:candidate")
                .build());
        schema.refresh(metadataKeyTable().edit().setPrimaryKeyNames("candidate").create());
        assertThat(schema.registerTableFromBinlogMetadata(metadataKeyTableMap())).isTrue();
        assertThat(schema.schemaFor(METADATA_KEY_TABLE_ID).keySchema().fields())
                .extracting(Field::name).containsExactly("candidate");
    }

    @Test
    void shouldPreserveDecodedEnumAndSetMetadataValues() {
        schema = getSchema(metadataKeyConfig().build());
        final String[] options = { "plain", "a''b", "'quoted'", "\"quoted\"", "a\\'b", "a\\nb", "`quoted`", "" };
        final TableMapEventData metadata = metadataKeyTableMap();
        metadata.getEventMetadata().setColumnNames(List.of("id", "e", "s"));
        metadata.getEventMetadata().setEnumStrValues(List.<String[]> of(options));
        metadata.getEventMetadata().setSetStrValues(List.<String[]> of(options));
        metadata.setColumnTypes(new byte[]{ 3, (byte) 247, (byte) 248 });
        metadata.setColumnMetadata(new int[]{ 0, 1, 1 });
        metadata.setColumnNullability(new BitSet());
        assertThat(schema.registerTableFromBinlogMetadata(metadata)).isTrue();

        final var tableSchema = schema.schemaFor(METADATA_KEY_TABLE_ID);
        for (int i = 0; i < options.length; i++) {
            final var value = tableSchema.valueFromColumnData(new Object[]{ i, i + 1, 1L << i });
            for (final var column : List.of("e", "s")) {
                assertThat(value.getString(column)).as("Decoded %s option %s", column, i).isEqualTo(options[i]);
                assertThat(value.schema().field(column).schema().parameters().get("allowed"))
                        .isEqualTo(EnumeratedValues.toCommaSeparatedString(List.of(options)));
            }
        }
    }

    private Configuration.Builder metadataKeyConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.BINLOG_METADATA_BASED_SCHEMA, true)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, "connector_test.metadataKeys");
    }

    private Table metadataKeyTable() {
        return Table.editor().tableId(METADATA_KEY_TABLE_ID)
                .addColumn(Column.editor().name("id").position(1).type("INT").jdbcType(Types.INTEGER).optional(false).create())
                .addColumn(Column.editor().name("candidate").position(2).type("INT").jdbcType(Types.INTEGER).optional(true).create())
                .create();
    }

    private TableMapEventData metadataKeyTableMap() {
        final TableMapEventMetadata metadata = new TableMapEventMetadata();
        metadata.setColumnNames(List.of("id", "candidate"));
        metadata.setSimplePrimaryKeys(List.of(0));
        final TableMapEventData data = new TableMapEventData();
        data.setDatabase(METADATA_KEY_TABLE_ID.catalog());
        data.setTable(METADATA_KEY_TABLE_ID.table());
        data.setColumnTypes(new byte[]{ 3, 3 });
        data.setColumnMetadata(new int[]{ 0, 0 });
        final BitSet nullability = new BitSet();
        nullability.set(1);
        data.setColumnNullability(nullability);
        data.setEventMetadata(metadata);
        return data;
    }

    @AfterEach
    void afterEach() {
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
    void shouldApplyDdlStatementsAndRecover() throws InterruptedException {
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
    void shouldIgnoreUnparseableDdlAndRecover() throws InterruptedException {
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

    @Test
    public void shouldFailOnUnparseableDdl() throws InterruptedException {
        assertThrows(ParsingException.class, () -> {
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
        });
    }

    @Test
    void shouldLoadSystemAndNonSystemTablesAndConsumeOnlyFilteredDatabases() throws InterruptedException {
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
    void shouldAllowDecimalPrecision() throws InterruptedException {
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
    void addCommentToSchemaTest() {
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
