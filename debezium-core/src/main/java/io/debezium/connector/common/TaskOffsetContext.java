/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.debezium.pipeline.spi.OffsetContext;

public class TaskOffsetContext<P extends TaskPartition, O extends OffsetContext> {
    private final Map<P, O> offsets;

    public TaskOffsetContext(Map<P, O> offsets) {
        this.offsets = offsets;
    }

    public Map<P, O> getOffsets() {
        return offsets;
    }

    /**
     * Loads connector task context.
     */
    public static class Loader<P extends TaskPartition, O extends OffsetContext, L extends OffsetContext.Loader<O>> {

        private final OffsetStorageReader offsetReader;
        private final L offsetLoader;

        public Loader(OffsetStorageReader offsetReader, L offsetLoader) {
            this.offsetReader = offsetReader;
            this.offsetLoader = offsetLoader;
        }

        /**
         * Given the collection of connector-specific task partitions, returns their respective connector-specific offsets.
         */
        public TaskOffsetContext<P, O> load(Collection<P> taskPartitions) {
            Collection<Map<String, String>> sourcePartitions = taskPartitions.stream()
                    .map(TaskPartition::getSourcePartition)
                    .collect(Collectors.toCollection(HashSet::new));

            Map<Map<String, String>, Map<String, Object>> sourceOffsets = offsetReader.offsets(sourcePartitions);

            Map<P, O> taskOffsets = new HashMap<>();
            taskPartitions.forEach(taskPartition -> {
                Map<String, String> sourcePartition = taskPartition.getSourcePartition();
                Map<String, Object> sourceOffset = sourceOffsets.get(sourcePartition);
                O taskOffset = null;
                if (sourceOffset != null) {
                    taskOffset = offsetLoader.load(taskPartition.getSourcePartition(), sourceOffset);
                }
                taskOffsets.put(taskPartition, taskOffset);
            });

            return new TaskOffsetContext<>(taskOffsets);
        }
    }
}
