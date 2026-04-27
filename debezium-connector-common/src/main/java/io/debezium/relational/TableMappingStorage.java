/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Interface for storing mappings between {@link TableId} and associated data.
 * This abstraction allows for alternative storage implementations to reduce memory pressure.
 *
 * @param <V> the type of values stored
 * @author Debezium Authors
 */
public interface TableMappingStorage<V> {

    /**
     * Configures this storage instance with connector configuration.
     * This method is called once after instantiation and before any other methods.
     *
     * @param config the connector configuration
     * @param tableIdCaseInsensitive whether table identifiers should be treated case-insensitively
     */
    void configure(RelationalDatabaseConnectorConfig config, boolean tableIdCaseInsensitive);

    /**
     * Retrieves the value associated with the given table identifier.
     *
     * @param tableId the table identifier
     * @return the associated value, or null if not present
     */
    V get(TableId tableId);

    /**
     * Associates the specified value with the specified table identifier.
     *
     * @param tableId the table identifier
     * @param value the value to be associated
     * @return the previous value associated with the table identifier, or null if there was no mapping
     */
    V put(TableId tableId, V value);

    /**
     * Removes the mapping for the specified table identifier.
     *
     * @param tableId the table identifier
     * @return the previous value associated with the table identifier, or null if there was no mapping
     */
    V remove(TableId tableId);

    /**
     * Removes all mappings from this storage.
     */
    void clear();

    /**
     * Returns the number of table identifier-value mappings in this storage.
     *
     * @return the number of mappings
     */
    int size();

    /**
     * Returns true if this storage contains no mappings.
     *
     * @return true if this storage contains no mappings
     */
    boolean isEmpty();

    /**
     * Returns a set view of the table identifiers contained in this storage.
     *
     * @return a set view of the table identifiers
     */
    Set<TableId> keySet();

    /**
     * Performs the given action for each entry in this storage until all entries
     * have been processed or the action throws an exception.
     *
     * @param action the action to be performed for each entry
     */
    void forEach(BiConsumer<? super TableId, ? super V> action);
}
