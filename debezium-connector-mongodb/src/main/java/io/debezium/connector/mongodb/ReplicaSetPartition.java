/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Map;
import java.util.Objects;

import io.debezium.util.Collect;

public class ReplicaSetPartition extends MongoDbPartition {
    private static final String SERVER_ID_KEY = "server_id";
    private static final String REPLICA_SET_NAME = "rs";

    private final String serverId;
    private final String replicaSetName;

    public ReplicaSetPartition(String serverId, String replicaSetName) {
        this.serverId = serverId;
        this.replicaSetName = replicaSetName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_ID_KEY, serverId, REPLICA_SET_NAME, replicaSetName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ReplicaSetPartition other = (ReplicaSetPartition) obj;
        return Objects.equals(serverId, other.serverId) && Objects.equals(replicaSetName, other.replicaSetName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, replicaSetName);
    }

    @Override
    public String toString() {
        return "ReplicaSetPartition [sourcePartition=" + getSourcePartition() + "]";
    }
}
