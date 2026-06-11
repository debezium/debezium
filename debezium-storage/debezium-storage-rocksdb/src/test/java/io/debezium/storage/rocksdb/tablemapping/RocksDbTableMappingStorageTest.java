/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.tablemapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.TableMappingStorage;

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
