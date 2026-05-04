/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Set;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.BoundedConcurrentHashMap;

/**
 * Abstract base class for {@link TableMappingStorage} implementations that provides caching functionality.
 * Subclasses need only implement the actual storage operations.
 *
 * @param <V> the type of values stored (must be Serializable)
 * @author Debezium Authors
 */
public abstract class AbstractCachedTableMappingStorage<V> implements TableMappingStorage<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCachedTableMappingStorage.class);
    protected static final String CACHE_SIZE_CONFIG = "memory.management.cache.size";
    protected static final int DEFAULT_CACHE_SIZE = 1000;
    protected boolean tableIdCaseInsensitive;
    private BoundedConcurrentHashMap<TableId, V> cache;

    @Override
    public void configure(RelationalDatabaseConnectorConfig config, boolean tableIdCaseInsensitive) {
        this.tableIdCaseInsensitive = tableIdCaseInsensitive;
        // Initialize cache
        String cacheSizeConfig = config.getConfig().getString(CACHE_SIZE_CONFIG);
        int cacheSize = cacheSizeConfig != null ? Integer.parseInt(cacheSizeConfig) : DEFAULT_CACHE_SIZE;
        cache = new BoundedConcurrentHashMap<>(cacheSize);
        LOGGER.info("Initialized table mapping storage with cache size: {}", cacheSize);
        // Allow subclasses to perform their own configuration
        configureStorage(config);
    }

    @Override
    public V get(TableId tableId) {
        TableId normalizedTableId = normalizeTableId(tableId);
        // Check cache first
        V cachedValue = cache.get(normalizedTableId);
        if (cachedValue != null) {
            return cachedValue;
        }
        // Cache miss - fetch from storage
        V value = getFromStorage(normalizedTableId);
        if (value != null) {
            // Update cache
            cache.put(normalizedTableId, value);
        }
        return value;
    }

    @Override
    public V put(TableId tableId, V value) {
        TableId normalizedTableId = normalizeTableId(tableId);
        V oldValue = get(tableId);
        putToStorage(normalizedTableId, value);
        // Update cache
        cache.put(normalizedTableId, value);
        return oldValue;
    }

    @Override
    public V remove(TableId tableId) {
        TableId normalizedTableId = normalizeTableId(tableId);
        V oldValue = get(tableId);
        removeFromStorage(normalizedTableId);
        // Remove from cache
        cache.remove(normalizedTableId);
        return oldValue;
    }

    @Override
    public void clear() {
        // Clear cache
        cache.clear();
        // Clear storage
        clearStorage();
    }

    /**
     * Normalizes the table identifier based on case sensitivity settings.
     */
    protected TableId normalizeTableId(TableId tableId) {
        return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
    }

    /**
     * Configures the underlying storage implementation.
     * Called during {@link #configure(RelationalDatabaseConnectorConfig, boolean)}.
     *
     * @param config the connector configuration
     */
    protected abstract void configureStorage(RelationalDatabaseConnectorConfig config);

    /**
     * Retrieves a value from the underlying storage.
     *
     * @param tableId the normalized table identifier
     * @return the value, or null if not found
     */
    protected abstract V getFromStorage(TableId tableId);

    /**
     * Stores a value in the underlying storage.
     *
     * @param tableId the normalized table identifier
     * @param value the value to store
     */
    protected abstract void putToStorage(TableId tableId, V value);

    /**
     * Removes a value from the underlying storage.
     *
     * @param tableId the normalized table identifier
     */
    protected abstract void removeFromStorage(TableId tableId);

    /**
     * Clears all entries from the underlying storage.
     */
    protected abstract void clearStorage();

    @Override
    public abstract int size();

    @Override
    public abstract boolean isEmpty();

    @Override
    public abstract Set<TableId> keySet();

    @Override
    public abstract void forEach(BiConsumer<? super TableId, ? super V> action);
}
