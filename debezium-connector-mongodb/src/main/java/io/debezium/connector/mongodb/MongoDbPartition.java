/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.debezium.pipeline.spi.Partition;

/**
 * Represents partition used by MongoDB connector
 * TODO: replicaSetName is preserved only for offset consolidation
 */
public class MongoDbPartition implements Partition {
    private static final String SERVER_ID_KEY = "server_id";
    private static final String REPLICA_SET_NAME = "rs";
    private static final String TASK_ID_KEY = "task_id";
    private static final String MULTI_TASK_GEN_KEY = "multi_task_gen";
    private static final String TASK_COUNT_KEY = "task_count";

    private final String serverId;
    private final String replicaSetName;

    private final boolean multiTaskEnabled;
    private final int multiTaskGen;
    private final int taskId;
    private final int taskCount;

    public MongoDbPartition(String serverId) {
        this.serverId = serverId;
        this.replicaSetName = null;
        this.multiTaskGen = -1;
        this.taskId = 0;
        this.taskCount = 0;
        this.multiTaskEnabled = false;
    }

    /**
     * This should not be used outside of {@link MongoDbConnectorTask#getPreviousOffsets(MongoDbConnectorConfig)}
     */
    MongoDbPartition(String serverId, String replicaSetName) {
        this.serverId = serverId;
        this.replicaSetName = replicaSetName;
        this.multiTaskGen = -1;
        this.taskId = 0;
        this.taskCount = 1;
        this.multiTaskEnabled = false;
    }

    MongoDbPartition(String serverId, String replicaSetName, boolean multiTaskEnabled, int multiTaskGen, int taskId, int taskCount) {
        this.serverId = serverId;
        this.replicaSetName = replicaSetName;
        this.multiTaskEnabled = multiTaskEnabled;
        this.multiTaskGen = multiTaskGen;
        this.taskId = taskId;
        this.taskCount = taskCount;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        Map<String, String> partition = new HashMap<>();
        if (this.multiTaskEnabled) {
            partition.put(TASK_ID_KEY, String.valueOf(this.taskId));
            partition.put(MULTI_TASK_GEN_KEY, String.valueOf(this.multiTaskGen));
            partition.put(TASK_COUNT_KEY, String.valueOf(taskCount));
        }
        if (replicaSetName != null) {
            partition.put(REPLICA_SET_NAME, replicaSetName);
        }
        partition.put(SERVER_ID_KEY, serverId);
        return partition;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final MongoDbPartition other = (MongoDbPartition) obj;
        if (multiTaskEnabled) {
            return Objects.equals(serverId, other.serverId) && Objects.equals(replicaSetName, other.replicaSetName)
                    && multiTaskEnabled == other.multiTaskEnabled && multiTaskGen == other.multiTaskGen
                    && taskId == other.taskId && taskCount == other.taskCount;
        }
        return Objects.equals(serverId, other.serverId) && Objects.equals(replicaSetName, other.replicaSetName) && multiTaskEnabled == other.multiTaskEnabled;
    }

    @Override
    public int hashCode() {
        if (multiTaskEnabled) {
            return Objects.hash(serverId, replicaSetName, multiTaskGen, taskId, taskCount);
        }
        return Objects.hash(serverId, replicaSetName);
    }

    @Override
    public String toString() {
        return "MongoDbPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    /**
     * Provider of partitions used by MongoDB connector
     * TODO: replicaSetNames are preserved only for offset consolidation
     */
    public static class Provider implements Partition.Provider<MongoDbPartition> {
        private final String logicalName;
        private final Set<String> replicaSetNames;
        private final boolean multiTaskEnabled;
        private final int generation;
        private final int taskCount;
        private final int taskId;

        public Provider(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext) {
            this(connectorConfig, taskContext, Set.of());
        }

        /**
         * This should not be used outside of {@link MongoDbConnectorTask#getPreviousOffsets(MongoDbConnectorConfig)}
         */
        Provider(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext, Set<String> replicaSetNames) {
            this.logicalName = connectorConfig.getLogicalName();
            this.replicaSetNames = replicaSetNames;
            multiTaskEnabled = connectorConfig.isMultiTaskEnabled();
            generation = connectorConfig.getMultiTaskGen();
            taskCount = connectorConfig.getMaxTasks();
            taskId = taskContext.getMongoTaskId();
        }

        @Override
        public Set<MongoDbPartition> getPartitions() {
            if (replicaSetNames.isEmpty()) {
                return Set.of(new MongoDbPartition(logicalName, null, multiTaskEnabled, generation, taskId, taskCount));
            }
            return replicaSetNames.stream()
                    .map(name -> new MongoDbPartition(logicalName, name, multiTaskEnabled, generation, taskId, taskCount))
                    .collect(Collectors.toSet());
        }
    }

    public static class PrevGenProvider extends Provider {
        private final int prevGen;
        private final int prevMaxTasks;

        public PrevGenProvider(MongoDbConnectorConfig connectorConfig, MongoDbTaskContext taskContext) {
            super(connectorConfig, taskContext);
            prevGen = connectorConfig.getMultiTaskPrevGen();
            prevMaxTasks = connectorConfig.getMultiTaskPrevMaxTasks();
        }

        @Override
        public Set<MongoDbPartition> getPartitions() {
            var partitions = super.getPartitions();
            Set<MongoDbPartition> prevPartitions;
            if (prevGen < 0) {
                // There is no previous generation, fetch non-multitask offsets
                prevPartitions = partitions.stream()
                        .map(partition -> new MongoDbPartition(partition.serverId, partition.replicaSetName))
                        .collect(Collectors.toSet());
            }
            else {
                prevPartitions = partitions.stream()
                        .flatMap(partition -> IntStream.range(0, prevMaxTasks)
                                .mapToObj(taskId -> new MongoDbPartition(
                                        partition.serverId,
                                        partition.replicaSetName,
                                        true,
                                        prevGen,
                                        taskId,
                                        prevMaxTasks)))
                        .collect(Collectors.toSet());
            }
            return prevPartitions;
        }
    }
}
