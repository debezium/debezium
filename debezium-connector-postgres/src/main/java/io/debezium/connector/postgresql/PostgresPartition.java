/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class PostgresPartition extends AbstractPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;
    private final int taskId;

    public PostgresPartition(String serverName, String databaseName, int taskId) {
        super(databaseName);
        this.serverName = serverName;
        this.taskId = taskId;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, getPartitionIdentificationKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PostgresPartition other = (PostgresPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "PostgresPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    public String getPartitionIdentificationKey() {
        return String.format("%s_%d", serverName, taskId);
    }

    static class Provider implements Partition.Provider<PostgresPartition> {
        private final PostgresConnectorConfig connectorConfig;
        private final Configuration taskConfig;

        Provider(PostgresConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }

        @Override
        public Set<PostgresPartition> getPartitions() {
            return Collections.singleton(new PostgresPartition(
                    connectorConfig.getLogicalName(), taskConfig.getString(DATABASE_NAME.name()),
                    connectorConfig.taskId()));
        }
    }
}
