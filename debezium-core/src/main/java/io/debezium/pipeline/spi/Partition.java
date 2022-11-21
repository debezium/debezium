/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Describes the source partition to be processed by the connector in connector-specific terms
 * and provides its representation as a Kafka Connect source partition.
 */
public interface Partition {
    Map<String, String> getSourcePartition();

    default Map<String, String> getFallbackPartition() {
        return null;
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
