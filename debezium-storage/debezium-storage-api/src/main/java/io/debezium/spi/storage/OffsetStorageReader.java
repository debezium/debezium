/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.storage;

import java.util.Collection;
import java.util.Map;

import io.debezium.common.annotation.Incubating;

/**
 * Interface for reading offset data from storage.
 * This interface provides read-only access to stored connector offsets.
 *
 * @author Debezium Authors
 */
@Incubating
public interface OffsetStorageReader {

    /**
     * Get the offset for a single partition.
     *
     * @param partition the partition map (typically contains table/collection identifiers)
     * @param <T> the type of the partition key values
     * @return the offset map, or null if no offset exists for this partition
     */
    <T> Map<String, Object> offset(Map<String, T> partition);

    /**
     * Get the offsets for multiple partitions.
     *
     * @param partitions collection of partition maps
     * @param <T> the type of the partition key values
     * @return map of partition to offset for each partition that has a stored offset
     */
    <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions);
}
