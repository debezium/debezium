/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes the source partition to be processed by the connector in connector-specific terms
 * and provides its representation as a Kafka Connect source partition.
 */
public interface Partition {

    /**
     * Get source partition representation in current format, the most recent one.
     *
     * @return map being representation of this source partition
     */
    Map<String, String> getSourcePartition();

    /**
     * Get all representations of the source partition in all supported formats.
     * The list includes current format, as the first one - the most preferred, but also
     * all legacy formats that are still supported, in descending order of preference.
     *
     * @return list of maps, each map being representation of this source partition
     */
    default List<Map<String, String>> getSupportedFormats() {
        return Collections.singletonList(getSourcePartition());
    }

    /**
     * Returns the partition representation in the logging context.
     */
    default Map<String, String> getLoggingContext() {
        return Collections.emptyMap();
    }

    /**
     * Implementations provide a set of connector-specific partitions based on the connector task configuration.
     */
    interface Provider<P extends Partition> {
        Set<P> getPartitions();
    }
}
