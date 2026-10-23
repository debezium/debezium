/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.tablemapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.doc.FixFor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ExtendedMetadata;
import io.debezium.relational.ExtendedMetadata.ExtendedColumn;
import io.debezium.relational.ExtendedMetadata.ExtendedRelationalMetadataFactory;
import io.debezium.relational.ExtendedMetadata.ExtendedTable;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalMetadataFactory;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableImpl;
import io.debezium.relational.TableMappingStorage;
import io.debezium.storage.rocksdb.RocksDbUtil;

/**
 * Unit test for RocksDb-based table mapping storage.
 * Tests storage operations, cache behavior, and persistence across restarts.
 *
 * @author Debezium Authors
 */
public class RocksDbTableMappingStorageTest {

    private static final Path ROCKSDB_PATH = Path.of(System.getProperty("java.io.tmpdir"), "rocksdb-storage-test");

    private RocksDbTableMappingStorage<String> storage;

    @BeforeEach
    public void beforeEach() throws IOException {
        // Clean up any existing test directory
        deleteDirectory(ROCKSDB_PATH);
    }

    @AfterEach
    public void afterEach() throws IOException {
        if (storage != null) {
            storage.close();
            storage = null;
        }
        deleteDirectory(ROCKSDB_PATH);
    }

    @Test
    public void shouldStoreAndRetrieveValues() {
        // Create storage with cache size of 1 to force RocksDb usage
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");
        final TableId table3 = new TableId("catalog", "schema", "table3");
        final TableId table4 = new TableId("catalog", "schema", "table4");

        // Store values
        storage.put(table1, "value1");
        storage.put(table2, "value2");
        storage.put(table3, "value3");
        storage.put(table4, "value4");

        // Verify retrieval
        assertThat(storage.get(table1)).isEqualTo("value1");
        assertThat(storage.get(table2)).isEqualTo("value2");
        assertThat(storage.get(table3)).isEqualTo("value3");
        assertThat(storage.get(table4)).isEqualTo("value4");

        // Verify size
        assertThat(storage.size()).isEqualTo(4);
    }

    @Test
    public void shouldUseCacheForFrequentlyAccessedItems() {
        // Create storage with cache size of 1
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");

        // Store values
        storage.put(table1, "value1");
        storage.put(table2, "value2");

        // Access table1 multiple times - should be cached
        assertThat(storage.get(table1)).isEqualTo("value1");
        assertThat(storage.get(table1)).isEqualTo("value1");
        assertThat(storage.get(table1)).isEqualTo("value1");

        // Access table2 - should evict table1 from cache due to size limit
        assertThat(storage.get(table2)).isEqualTo("value2");

        // Access table1 again - should still work (retrieved from RocksDb)
        assertThat(storage.get(table1)).isEqualTo("value1");
    }

    @Test
    public void shouldPersistDataAcrossRestarts() {
        // Create storage with cleanup disabled
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");
        final TableId table3 = new TableId("catalog", "schema", "table3");
        final TableId table4 = new TableId("catalog", "schema", "table4");

        // Store values
        storage.put(table1, "value1");
        storage.put(table2, "value2");
        storage.put(table3, "value3");
        storage.put(table4, "value4");

        // Verify RocksDb directory exists
        assertThat(ROCKSDB_PATH.toFile().exists()).isTrue();
        assertThat(ROCKSDB_PATH.toFile().isDirectory()).isTrue();

        // Close storage
        storage.close();

        // Verify data persists
        assertThat(ROCKSDB_PATH.toFile().exists()).isTrue();

        // Create new storage instance (simulating restart)
        storage = createStorage(1, false);

        // Verify data is still available
        assertThat(storage.get(table1)).isEqualTo("value1");
        assertThat(storage.get(table2)).isEqualTo("value2");
        assertThat(storage.get(table3)).isEqualTo("value3");
        assertThat(storage.get(table4)).isEqualTo("value4");
        assertThat(storage.size()).isEqualTo(4);
    }

    @Test
    public void shouldClearStorage() {
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");

        // Store values
        storage.put(table1, "value1");
        storage.put(table2, "value2");

        assertThat(storage.size()).isEqualTo(2);

        // Clear storage
        storage.clear();

        // Verify storage is empty
        assertThat(storage.size()).isEqualTo(0);
        assertThat(storage.isEmpty()).isTrue();
        assertThat(storage.get(table1)).isNull();
        assertThat(storage.get(table2)).isNull();
    }

    @Test
    public void shouldRemoveValues() {
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");

        // Store values
        storage.put(table1, "value1");
        storage.put(table2, "value2");

        assertThat(storage.size()).isEqualTo(2);

        // Remove one value
        storage.remove(table1);

        // Verify removal
        assertThat(storage.size()).isEqualTo(1);
        assertThat(storage.get(table1)).isNull();
        assertThat(storage.get(table2)).isEqualTo("value2");
    }

    @Test
    public void shouldIterateOverKeys() {
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");
        final TableId table3 = new TableId("catalog", "schema", "table3");
        final TableId table4 = new TableId("catalog", "schema", "table4");

        // Store values
        storage.put(table1, "value1");
        storage.put(table2, "value2");
        storage.put(table3, "value3");
        storage.put(table4, "value4");

        // Get keys
        final Set<TableId> keys = storage.keySet();

        // Verify keys
        assertThat(keys).hasSize(4);
        assertThat(keys).contains(table1, table2, table3, table4);
    }

    @Test
    public void shouldCleanupOnCloseWhenEnabled() throws IOException {
        // Create storage with cleanup enabled
        storage = createStorage(1, true);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        storage.put(table1, "value1");

        // Verify storage directory exists
        assertThat(ROCKSDB_PATH.toFile().exists()).isTrue();

        // Close storage
        storage.close();
        storage = null;

        // Verify storage directory was cleaned up
        assertThat(ROCKSDB_PATH.toFile().exists()).isFalse();
    }

    @Test
    public void shouldCleanupOnCloseByDefault() throws IOException {
        // Create storage without specifying cleanup config (should default to true)
        storage = createStorageWithDefaultCleanup(1);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        storage.put(table1, "value1");

        // Verify storage directory exists
        assertThat(ROCKSDB_PATH.toFile().exists()).isTrue();

        // Close storage
        storage.close();
        storage = null;

        // Verify storage directory was cleaned up by default
        assertThat(ROCKSDB_PATH.toFile().exists()).isFalse();
    }

    @Test
    public void shouldHandleMultipleTablesWithCacheSizeOne() {
        // This test specifically verifies that with cache size 1, RocksDb is used
        storage = createStorage(1, false);

        final TableId table1 = new TableId("catalog", "schema", "table1");
        final TableId table2 = new TableId("catalog", "schema", "table2");
        final TableId table3 = new TableId("catalog", "schema", "table3");
        final TableId table4 = new TableId("catalog", "schema", "table4");

        // Store all values
        storage.put(table1, "value1");
        storage.put(table2, "value2");
        storage.put(table3, "value3");
        storage.put(table4, "value4");

        // Access in different order to test cache eviction
        assertThat(storage.get(table4)).isEqualTo("value4");
        assertThat(storage.get(table1)).isEqualTo("value1");
        assertThat(storage.get(table3)).isEqualTo("value3");
        assertThat(storage.get(table2)).isEqualTo("value2");

        // All values should still be accessible
        assertThat(storage.get(table1)).isEqualTo("value1");
        assertThat(storage.get(table2)).isEqualTo("value2");
        assertThat(storage.get(table3)).isEqualTo("value3");
        assertThat(storage.get(table4)).isEqualTo("value4");
    }

    @Test
    public void shouldStoreAndRetrieveTablesWithDefaultMetadataFactory() throws Exception {
        final RocksDbTableMappingStorage<Table> tableStorage = createTableStorage(new MySqlConnectorConfig(baseConfiguration(1, false)));
        try {
            final TableId tableId = new TableId("catalog", null, "orders");
            final Table table = Table.editor()
                    .tableId(tableId)
                    .addColumn(Column.editor().name("id").jdbcType(Types.NUMERIC).type("NUMERIC").length(10).scale(0).optional(false).create())
                    .addColumn(Column.editor().name("status").jdbcType(Types.VARCHAR).type("VARCHAR").length(20)
                            .defaultValueExpression("'NEW'").enumValues(java.util.List.of("NEW", "PAID")).comment("state").create())
                    .setPrimaryKeyNames("id")
                    .setDefaultCharsetName("utf8")
                    .addAttribute(Attribute.editor().name("object_id").value("12345").create())
                    .create();

            tableStorage.put(tableId, table);
            // A second entry pushes the first out of the single-slot cache so the next read goes to RocksDB
            tableStorage.put(new TableId("catalog", null, "other"), Table.editor().tableId(new TableId("catalog", null, "other")).create());

            final Table stored = tableStorage.get(tableId);
            assertThat(stored).isExactlyInstanceOf(TableImpl.class);
            assertThat(stored).isEqualTo(table);
            assertThat(stored.id()).isEqualTo(tableId);
            assertThat(stored.attributes()).isEqualTo(table.attributes());
            assertThat(stored.columnWithName("status").defaultValueExpression()).contains("'NEW'");
            assertThat(stored.columnWithName("status").enumValues()).containsExactly("NEW", "PAID");
            assertThat(stored.columnWithName("status").comment()).isEqualTo("state");
        }
        finally {
            tableStorage.close();
        }
    }

    @Test
    public void shouldStoreAndRetrieveTablesThroughConfiguredMetadataFactory() throws Exception {
        final RelationalDatabaseConnectorConfig config = new TestRelationalDatabaseConfig(baseConfiguration(1, false), null, null, 0) {
            @Override
            public RelationalMetadataFactory getRelationalMetadataFactory() {
                return ExtendedRelationalMetadataFactory.INSTANCE;
            }
        };
        final RocksDbTableMappingStorage<Table> tableStorage = createTableStorage(config);
        try {
            final TableId tableId = new TableId(null, "schema", "orders");
            tableStorage.put(tableId, ExtendedMetadata.table(tableId, "orders-label", "eu"));
            tableStorage.put(new TableId(null, "schema", "other"), ExtendedMetadata.table(new TableId(null, "schema", "other"), "x", "us"));

            final Table stored = tableStorage.get(tableId);
            assertThat(stored).isInstanceOf(ExtendedTable.class);
            assertThat(stored.id()).isEqualTo(tableId);
            assertThat(stored.unwrap(ExtendedTable.class).label()).isEqualTo("orders-label");
            assertThat(stored.columnWithName("id").unwrap(ExtendedColumn.class).region()).isEqualTo("eu");
        }
        finally {
            tableStorage.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2843")
    public void shouldKeepTableIdCaseWhenTableIdsAreCaseInsensitive() throws Exception {
        final TableId tableId = new TableId("Catalog", null, "Orders");
        final TableId otherId = new TableId("Catalog", null, "Other");

        final RocksDbTableMappingStorage<Table> first = createTableStorage(new MySqlConnectorConfig(baseConfiguration(1, false)), true);
        try {
            first.put(tableId, Table.editor().tableId(tableId).create());
            // A second entry pushes the first out of the single-slot cache so the next read goes to RocksDB
            first.put(otherId, Table.editor().tableId(otherId).create());

            assertThat(first.get(new TableId("Catalog", null, "ORDERS")).id()).isEqualTo(tableId);

            final Set<TableId> storedIds = new HashSet<>();
            first.forEach((key, table) -> storedIds.add(table.id()));
            assertThat(storedIds).containsExactlyInAnyOrder(tableId, otherId);
        }
        finally {
            first.close();
        }

        final RocksDbTableMappingStorage<Table> restarted = createTableStorage(new MySqlConnectorConfig(baseConfiguration(1, false)), true);
        try {
            assertThat(restarted.get(tableId).id()).isEqualTo(tableId);
        }
        finally {
            restarted.close();
        }
    }

    @Test
    public void shouldReadTablesStoredInTheEarlierBinaryFormat() throws Exception {
        final TableId tableId = new TableId("catalog", null, "legacy");

        // Write an entry in the binary layout used before tables were stored as JSON
        final RocksDbTableMappingStorage<Table> first = createTableStorage(new MySqlConnectorConfig(baseConfiguration(1, false)));
        first.close();
        try (var options = RocksDbUtil.createOptions(); RocksDB db = RocksDB.open(options, ROCKSDB_PATH.toString())) {
            db.put(legacyKey(tableId), legacyTableValue(tableId));
        }

        final RocksDbTableMappingStorage<Table> tableStorage = createTableStorage(new MySqlConnectorConfig(baseConfiguration(1, false)));
        try {
            final Table stored = tableStorage.get(tableId);
            assertThat(stored.id()).isEqualTo(tableId);
            assertThat(stored.primaryKeyColumnNames()).containsExactly("id");
            assertThat(stored.defaultCharsetName()).isEqualTo("utf8");
            assertThat(stored.columnWithName("id").typeName()).isEqualTo("NUMERIC");
            assertThat(stored.columnWithName("id").length()).isEqualTo(10);
            assertThat(stored.columnWithName("id").isOptional()).isFalse();
            assertThat(stored.attributes()).containsExactly(Attribute.editor().name("object_id").value("12345").create());
        }
        finally {
            tableStorage.close();
        }
    }

    private static byte[] legacyKey(final TableId tableId) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            writeNullableString(dos, tableId.catalog());
            writeNullableString(dos, tableId.schema());
            dos.writeUTF(tableId.table());
            return baos.toByteArray();
        }
    }

    private static byte[] legacyTableValue(final TableId tableId) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(1); // table marker of the binary layout
            writeNullableString(dos, tableId.catalog());
            writeNullableString(dos, tableId.schema());
            dos.writeUTF(tableId.table());
            dos.writeInt(1); // one column
            dos.writeUTF("id"); // name
            dos.writeInt(1); // position
            dos.writeInt(Types.NUMERIC); // jdbcType
            dos.writeInt(-1); // nativeType
            dos.writeUTF("NUMERIC"); // typeName
            writeNullableString(dos, null); // typeExpression
            writeNullableString(dos, null); // charsetName
            dos.writeInt(10); // length
            dos.writeBoolean(true); // scale present
            dos.writeInt(0); // scale
            dos.writeBoolean(false); // optional
            dos.writeBoolean(false); // autoIncremented
            dos.writeBoolean(false); // generated
            dos.writeBoolean(false); // defaultValueExpression absent
            dos.writeBoolean(false); // hasDefaultValue
            dos.writeInt(0); // enum values
            writeNullableString(dos, null); // column comment
            dos.writeInt(1); // one primary key column
            dos.writeUTF("id");
            writeNullableString(dos, "utf8"); // default charset
            writeNullableString(dos, null); // table comment
            dos.writeInt(1); // one attribute
            dos.writeUTF("object_id");
            writeNullableString(dos, "12345");
            return baos.toByteArray();
        }
    }

    private static void writeNullableString(final DataOutputStream dos, final String value) throws IOException {
        if (value == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeUTF(value);
        }
    }

    private RocksDbTableMappingStorage<Table> createTableStorage(final RelationalDatabaseConnectorConfig config) {
        return createTableStorage(config, false);
    }

    private RocksDbTableMappingStorage<Table> createTableStorage(final RelationalDatabaseConnectorConfig config, final boolean tableIdCaseInsensitive) {
        final RocksDbTableMappingStorage<Table> newStorage = new RocksDbTableMappingStorage<>();
        newStorage.configure(config, tableIdCaseInsensitive, TableMappingStorage.Type.TABLES);
        return newStorage;
    }

    private Configuration baseConfiguration(final int cacheSize, final boolean cleanup) {
        return Configuration.create()
                .with("memory.management.cache.size", cacheSize)
                .with("memory.management.tables.rocksdb.path", ROCKSDB_PATH.toString())
                .with("memory.management.tables.rocksdb.cleanup", cleanup)
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "testuser")
                .with(MySqlConnectorConfig.PASSWORD, "testpw")
                .with(MySqlConnectorConfig.SERVER_ID, 12345)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test-server")
                .build();
    }

    private RocksDbTableMappingStorage<String> createStorage(final int cacheSize, final boolean cleanup) {
        final Configuration config = Configuration.create()
                .with("memory.management.cache.size", cacheSize)
                .with("memory.management.tables.rocksdb.path", ROCKSDB_PATH.toString())
                .with("memory.management.tables.rocksdb.cleanup", cleanup)
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "testuser")
                .with(MySqlConnectorConfig.PASSWORD, "testpw")
                .with(MySqlConnectorConfig.SERVER_ID, 12345)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test-server")
                .build();

        final RocksDbTableMappingStorage<String> newStorage = new RocksDbTableMappingStorage<>();
        newStorage.configure(new MySqlConnectorConfig(config), false, TableMappingStorage.Type.TABLES);
        return newStorage;
    }

    private RocksDbTableMappingStorage<String> createStorageWithDefaultCleanup(final int cacheSize) {
        final Configuration config = Configuration.create()
                .with("memory.management.cache.size", cacheSize)
                .with("memory.management.tables.rocksdb.path", ROCKSDB_PATH.toString())
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "testuser")
                .with(MySqlConnectorConfig.PASSWORD, "testpw")
                .with(MySqlConnectorConfig.SERVER_ID, 12345)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test-server")
                .build();

        final RocksDbTableMappingStorage<String> newStorage = new RocksDbTableMappingStorage<>();
        newStorage.configure(new MySqlConnectorConfig(config), false, TableMappingStorage.Type.TABLES);
        return newStorage;
    }

    private void deleteDirectory(final Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        }
                        catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

}
