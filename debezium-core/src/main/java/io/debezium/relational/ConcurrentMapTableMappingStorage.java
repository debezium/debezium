/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * Default implementation of {@link TableMappingStorage} using {@link ConcurrentHashMap}.
 * This implementation stores mappings in memory and handles case-insensitive table identifiers
 * by converting them to lowercase when needed.
 *
 * @param <V> the type of values stored
 * @author Debezium Authors
 */
public class ConcurrentMapTableMappingStorage<V> implements TableMappingStorage<V> {

    private boolean tableIdCaseInsensitive;
    private final ConcurrentMap<TableId, V> storage;

    /**
     * Creates a new storage instance.
     * Must call {@link #configure(RelationalDatabaseConnectorConfig, boolean)} before use.
     */
    public ConcurrentMapTableMappingStorage() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public void configure(RelationalDatabaseConnectorConfig config, boolean tableIdCaseInsensitive) {
        this.tableIdCaseInsensitive = tableIdCaseInsensitive;
    }

    @Override
    public V get(TableId tableId) {
        return storage.get(normalizeTableId(tableId));
    }

    @Override
    public V put(TableId tableId, V value) {
        return storage.put(normalizeTableId(tableId), value);
    }

    @Override
    public V remove(TableId tableId) {
        return storage.remove(normalizeTableId(tableId));
    }

    @Override
    public void clear() {
        storage.clear();
    }

    @Override
    public int size() {
        return storage.size();
    }

    @Override
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public Set<TableId> keySet() {
        return storage.keySet();
    }

    @Override
    public void forEach(BiConsumer<? super TableId, ? super V> action) {
        storage.forEach(action);
    }

    /**
     * Normalizes the table identifier based on case sensitivity settings.
     *
     * @param tableId the table identifier to normalize
     * @return the normalized table identifier
     */
    private TableId normalizeTableId(TableId tableId) {
        return tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
    }
}
