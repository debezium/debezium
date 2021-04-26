/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Collection;
import java.util.Map;

/**
 * Describes the partition of the source to be processed by the task and provides its representation
 * as a Kafka Connect source partition.
 */
public interface TaskPartition {
    Map<String, String> getSourcePartition();

    /**
     * Implementations provide task-specific partitions based on the task configuration.
     */
    interface Provider<P extends TaskPartition> {
        Collection<P> getPartitions();
    }
}
