/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.tablemapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.relational.AbstractCachedTableMappingStorage;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * RocksDB-based implementation of table mapping storage for persistent, disk-backed storage.
 * This implementation is suitable for large datasets that exceed available memory.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code memory.management.rocksdb.path} - Directory path for RocksDB storage (default: temp directory)</li>
 *   <li>{@code memory.management.rocksdb.cleanup} - Whether to delete RocksDB files on close (default: true)</li>
 *   <li>{@code memory.management.cache.size} - Cache size for frequently accessed entries (default: 1000)</li>
 * </ul>
 *
 * @param <V> the type of values stored (must be Serializable)
 * @author Debezium Authors
 */
public class RocksDBTableMappingStorage<V> extends AbstractCachedTableMappingStorage<V> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBTableMappingStorage.class);
    private static final String ROCKSDB_PATH_CONFIG = "memory.management.rocksdb.path";
    private static final String ROCKSDB_CLEANUP_CONFIG = "memory.management.rocksdb.cleanup";
    static {
        RocksDB.loadLibrary();
    }
    private RocksDB db;
    private Options options;
    private Path dbPath;
    private boolean cleanupOnClose = true;

    /**
     * Creates a new RocksDB storage instance.
     * Must call {@link #configure(RelationalDatabaseConnectorConfig, boolean)} before use.
     */
    public RocksDBTableMappingStorage() {
    }

    @Override
    protected void configureStorage(RelationalDatabaseConnectorConfig config) {
        try {
            // Determine storage path
            String configuredPath = config.getConfig().getString(ROCKSDB_PATH_CONFIG);
            if (configuredPath != null && !configuredPath.isEmpty()) {
                dbPath = Path.of(configuredPath);
            }
            else {
                dbPath = Files.createTempDirectory("debezium-rocksdb-");
            }
            // Check cleanup configuration
            String cleanupConfig = config.getConfig().getString(ROCKSDB_CLEANUP_CONFIG);
            if (cleanupConfig != null) {
                cleanupOnClose = Boolean.parseBoolean(cleanupConfig);
            }
            // Initialize RocksDB
            options = new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION);
            db = RocksDB.open(options, dbPath.toString());
            LOGGER.info("RocksDB table mapping storage initialized at: {}", dbPath);
        }
        catch (IOException | RocksDBException e) {
            throw new DebeziumException("Failed to initialize RocksDB storage", e);
        }
    }

    @Override
    protected V getFromStorage(TableId tableId) {
        try {
            byte[] key = serializeTableId(tableId);
            byte[] value = db.get(key);
            return value != null ? deserializeValue(value) : null;
        }
        catch (RocksDBException | IOException | ClassNotFoundException e) {
            throw new DebeziumException("Failed to get value from RocksDB", e);
        }
    }

    @Override
    protected void putToStorage(TableId tableId, V value) {
        try {
            byte[] key = serializeTableId(tableId);
            byte[] serializedValue = serializeValue(value);
            db.put(key, serializedValue);
        }
        catch (RocksDBException | IOException e) {
            throw new DebeziumException("Failed to put value into RocksDB", e);
        }
    }

    @Override
    protected void removeFromStorage(TableId tableId) {
        try {
            byte[] key = serializeTableId(tableId);
            db.delete(key);
        }
        catch (RocksDBException | IOException e) {
            throw new DebeziumException("Failed to remove value from RocksDB", e);
        }
    }

    @Override
    protected void clearStorage() {
        close();
        deleteDirectory(dbPath.toFile());
        try {
            Files.createDirectories(dbPath);
            db = RocksDB.open(options, dbPath.toString());
        }
        catch (IOException | RocksDBException e) {
            throw new DebeziumException("Failed to clear RocksDB storage", e);
        }
    }

    @Override
    public int size() {
        int count = 0;
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                count++;
                iterator.next();
            }
        }
        return count;
    }

    @Override
    public boolean isEmpty() {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            return !iterator.isValid();
        }
    }

    @Override
    public Set<TableId> keySet() {
        Set<TableId> keys = new HashSet<>();
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                TableId tableId = deserializeTableId(iterator.key());
                keys.add(tableId);
                iterator.next();
            }
        }
        catch (IOException | ClassNotFoundException e) {
            throw new DebeziumException("Failed to retrieve keys from RocksDB", e);
        }
        return keys;
    }

    @Override
    public void forEach(BiConsumer<? super TableId, ? super V> action) {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                TableId tableId = deserializeTableId(iterator.key());
                V value = deserializeValue(iterator.value());
                action.accept(tableId, value);
                iterator.next();
            }
        }
        catch (IOException | ClassNotFoundException e) {
            throw new DebeziumException("Failed to iterate over RocksDB entries", e);
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
        if (options != null) {
            options.close();
            options = null;
        }
        if (cleanupOnClose && dbPath != null) {
            deleteDirectory(dbPath.toFile());
            LOGGER.info("RocksDB storage cleaned up: {}", dbPath);
        }
    }

    /**
     * Serializes a TableId to bytes.
     */
    private byte[] serializeTableId(TableId tableId) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(tableId);
            return baos.toByteArray();
        }
    }

    /**
     * Deserializes bytes to a TableId.
     */
    private TableId deserializeTableId(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (TableId) ois.readObject();
        }
    }

    /**
     * Serializes a value to bytes.
     */
    private byte[] serializeValue(V value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            return baos.toByteArray();
        }
    }

    /**
     * Deserializes bytes to a value.
     */
    @SuppressWarnings("unchecked")
    private V deserializeValue(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (V) ois.readObject();
        }
    }

    /**
     * Recursively deletes a directory and its contents.
     */
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    }
                    else {
                        try {
                            Files.delete(file.toPath());
                        }
                        catch (IOException e) {
                            LOGGER.warn("Failed to delete file: {}", file, e);
                        }
                    }
                }
            }
            try {
                Files.delete(directory.toPath());
            }
            catch (IOException e) {
                LOGGER.warn("Failed to delete directory: {}", directory, e);
            }
        }
    }
}
