/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * Represents partition used by MongoDB connector
 * TODO: replicaSetName is preserved only for offset consolidation
 */
public class MongoDbPartition implements Partition {
    private static final String SERVER_ID_KEY = "server_id";
    private static final String REPLICA_SET_NAME = "rs";

    private final String serverId;
    private final String replicaSetName;

    public MongoDbPartition(String serverId) {
        this.serverId = serverId;
        this.replicaSetName = null;
    }

    /**
     * This should not be used outside of {@link MongoDbConnectorTask#getPreviousOffsets(MongoDbConnectorConfig)}
     */
    MongoDbPartition(String serverId, String replicaSetName) {
        this.serverId = serverId;
        this.replicaSetName = replicaSetName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return replicaSetName != null
                ? Collect.hashMapOf(SERVER_ID_KEY, serverId, REPLICA_SET_NAME, replicaSetName)
                : Collect.hashMapOf(SERVER_ID_KEY, serverId);
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
        return Objects.equals(serverId, other.serverId) && Objects.equals(replicaSetName, other.replicaSetName);
    }

    @Override
    public int hashCode() {
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

        public Provider(MongoDbConnectorConfig connectorConfig) {
            this(connectorConfig, Set.of());
        }

        /**
         * This should not be used outside of {@link MongoDbConnectorTask#getPreviousOffsets(MongoDbConnectorConfig)}
         */
        Provider(MongoDbConnectorConfig connectorConfig, Set<String> replicaSetNames) {
            this.logicalName = connectorConfig.getLogicalName();
            this.replicaSetNames = replicaSetNames;
        }

        @Override
        public Set<MongoDbPartition> getPartitions() {
            if (replicaSetNames.isEmpty()) {
                return Set.of(new MongoDbPartition(logicalName));
            }

            return replicaSetNames.stream()
                    .map(name -> new MongoDbPartition(logicalName, name))
                    .collect(Collectors.toSet());
        }
    }
}
