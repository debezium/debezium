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
    private static final String TASK_ID_KEY = "task_id";
    private static final String MULTI_TASK_GEN_KEY = "multi_task_gen";
    private final String serverId;
    private final String replicaSetName;
    private final boolean multiTaskEnabled;
    private final int taskId;
    private final int multiTaskGen;

    public ReplicaSetPartition(String serverId, String replicaSetName, boolean multiTaskEnabled, int taskId, int multiTaskGen) {
        this.serverId = serverId;
        this.replicaSetName = replicaSetName;
        this.taskId = taskId;
        this.multiTaskGen = multiTaskGen;
        this.multiTaskEnabled = multiTaskEnabled;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        if (this.multiTaskEnabled) {
            return Collect.hashMapOf(
                    SERVER_ID_KEY, serverId,
                    REPLICA_SET_NAME, replicaSetName,
                    TASK_ID_KEY, String.valueOf(this.taskId),
                    MULTI_TASK_GEN_KEY, String.valueOf(this.multiTaskGen));
        }
        else {
            return Collect.hashMapOf(
                    SERVER_ID_KEY, serverId,
                    REPLICA_SET_NAME, replicaSetName);
        }
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
        return Objects.equals(serverId, other.serverId) &&
                Objects.equals(replicaSetName, other.replicaSetName) &&
                Objects.equals(taskId, other.taskId) &&
                Objects.equals(multiTaskGen, other.multiTaskGen) &&
                Objects.equals(multiTaskEnabled, other.multiTaskEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, replicaSetName, taskId, multiTaskGen, multiTaskEnabled);
    }

    @Override
    public String toString() {
        return "ReplicaSetPartition [sourcePartition=" + getSourcePartition() + "]";
    }
}
